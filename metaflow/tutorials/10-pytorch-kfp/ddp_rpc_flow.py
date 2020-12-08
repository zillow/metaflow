import random
import socket
from time import sleep

from metaflow import FlowSpec, step, resources, S3


NUM_EMBEDDINGS = 100
EMBEDDING_DIM = 16


def _retrieve_embedding_parameters(emb_rref):
    from torch.distributed.rpc import RRef
    return [RRef(p) for p in emb_rref.local_value().parameters()]


def _run_trainer(emb_rref, rank, epochs=10):
    r"""
    Each trainer runs a forward pass which involves an embedding lookup on the
    parameter server and running nn.Linear locally. During the backward pass,
    DDP is responsible for aggregating the gradients for the dense part
    (nn.Linear) and distributed autograd ensures gradients updates are
    propagated to the parameter server.
    """
    import torch
    import torch.distributed.autograd as dist_autograd
    from torch.distributed.optim import DistributedOptimizer
    import torch.distributed.rpc as rpc
    from torch.distributed.rpc import RRef
    from torch.nn.parallel import DistributedDataParallel as DDP
    import torch.optim as optim

    class HybridModel(torch.nn.Module):
        r"""
        The model consists of a sparse part and a dense part. The dense part is an
        nn.Linear module that is replicated across all trainers using
        DistributedDataParallel. The sparse part is an nn.EmbeddingBag that is
        stored on the parameter server.

        The model holds a Remote Reference to the embedding table on the parameter
        server.
        """

        def __init__(self, emb_rref, device):
            super(HybridModel, self).__init__()
            self.emb_rref = emb_rref
            self.fc = DDP(torch.nn.Linear(16, 8).to(device), device_ids=[])
            self.device = device

        def forward(self, indices, offsets):
            emb_lookup = self.emb_rref.rpc_sync().forward(indices, offsets)
            return self.fc(emb_lookup.to(self.device))

    # Setup the model.
    model = HybridModel(emb_rref, "cpu")

    # Retrieve all model parameters as rrefs for DistributedOptimizer.

    # Retrieve parameters for embedding table.
    model_parameter_rrefs = rpc.rpc_sync(
            "ps", _retrieve_embedding_parameters, args=(emb_rref,))

    # model.parameters() only includes local parameters.
    for param in model.parameters():
        model_parameter_rrefs.append(RRef(param))

    # Setup distributed optimizer
    opt = DistributedOptimizer(
        optim.SGD,
        model_parameter_rrefs,
        lr=0.05,
    )
    print("setup distributed optimizer")

    criterion = torch.nn.CrossEntropyLoss()

    def get_next_batch(rank):
        for _ in range(10):
            num_indices = random.randint(20, 50)
            indices = torch.LongTensor(num_indices).random_(0, NUM_EMBEDDINGS)

            # Generate offsets.
            offsets = []
            start = 0
            batch_size = 0
            while start < num_indices:
                offsets.append(start)
                start += random.randint(1, 10)
                batch_size += 1

            offsets_tensor = torch.LongTensor(offsets)
            target = torch.LongTensor(batch_size).random_(8).to("cpu")
            yield indices, offsets_tensor, target

    # Train for 100 epochs
    for epoch in range(epochs):
        print("epoch", epoch)
        # create distributed autograd context
        for indices, offsets, target in get_next_batch(rank):
            print("batch", indices, offsets, target)
            with dist_autograd.context() as context_id:
                output = model(indices, offsets)
                loss = criterion(output, target)

                # Run distributed backward pass
                dist_autograd.backward(context_id, [loss])

                # Tun distributed optimizer
                opt.step(context_id)

                # Not necessary to zero grads as each iteration creates a different
                # distributed autograd context which hosts different grads
        print("Training done for epoch {}".format(epoch))


class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """
    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        # 2 trainers, 1 parameter server, 1 master.
        print("hi start")
        self.world_size = 4
        self.ranks = list(range(self.world_size))
        print("ranks", self.ranks)
        self.next(self.train, foreach="ranks")

    @resources(
        # cpu=0.5,  # cpu_limit=2,
        memory="1G", memory_limit="2G"
    )
    @step
    def train(self):
        """
        A step for metaflow to introduce itself.
        """
        print("hi train")
        print("...")
        import torch
        import torch.distributed as dist
        import torch.distributed.rpc as rpc
        from torch.distributed.rpc import TensorPipeRpcBackendOptions

        my_rank: int = self.input
        print("my_rank", my_rank)
        #
        rpc_backend_options = TensorPipeRpcBackendOptions()
        rpc_backend_options.init_method
        port = "5001"

        # Rank 2 is master, 3 is ps and 0 and 1 are trainers.
        if my_rank == 2:
            host = socket.gethostbyname(socket.gethostname())
            rpc_backend_options.init_method = f"tcp://{host}:{port}"

            with S3(run=self) as s3:
                print("saving host to s3", host)
                s3.put("master_host", host)

            rpc.init_rpc(
                "master",
                rank=my_rank,
                world_size=self.world_size,
                rpc_backend_options=rpc_backend_options)

            # Build the embedding table on the ps.
            emb_rref = rpc.remote(
                    "ps",
                    torch.nn.EmbeddingBag,
                    args=(NUM_EMBEDDINGS, EMBEDDING_DIM),
                    kwargs={"mode": "sum"})

            # Run the training loop on trainers.
            futs = []
            for trainer_rank in [0, 1]:
                trainer_name = "trainer{}".format(trainer_rank)
                fut = rpc.rpc_async(
                    trainer_name, _run_trainer, args=(emb_rref, my_rank, 10))
                futs.append(fut)

            # Wait for all training to finish.
            for fut in futs:
                fut.wait()
        elif my_rank <= 1:
            # Initialize process group for Distributed DataParallel on trainers.
            host = self.get_master_host()
            rpc_backend_options.init_method = f"tcp://{host}:{port}"

            # Initialize process group for Distributed DataParallel on trainers.
            dist.init_process_group(
                backend="gloo", rank=my_rank, world_size=2,  # TODO? self.world_size,
                init_method=f'tcp://{host}:5000')  # 29500

            # Initialize RPC.
            trainer_name = "trainer{}".format(my_rank)
            rpc.init_rpc(
                    trainer_name,
                    rank=my_rank,
                    world_size=self.world_size,
                    rpc_backend_options=rpc_backend_options)
        else:
            host = self.get_master_host()
            rpc_backend_options.init_method = f"tcp://{host}:{port}"
            rpc.init_rpc(
                    "ps",
                    rank=my_rank,
                    world_size=self.world_size,
                    rpc_backend_options=rpc_backend_options)
            # parameter server do nothing

        # block until all rpcs finish
        rpc.shutdown()

        self.next(self.join)

    def get_master_host(self):
        # return "localhost"
        # Initialize process group for Distributed DataParallel on trainers.
        with S3(run=self) as s3:
            exists = False
            while not exists:
                print(".")
                obj = s3.get("master_host", return_missing=True)
                exists = obj.exists
                if not exists:
                    sleep(1)
            host = obj.text
        print("host", host)
        return host

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == '__main__':
    HelloFlow()