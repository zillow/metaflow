from aip_kfp_sdk.components.pytorch import kfp_pytorch
from kfp.dsl import graph_component

from metaflow import FlowSpec, Parameter, step, kfp, current


def my_train_model_io(
    input_data_path: str = "/opt/zillow/mnist_pytorch_example/data",
    out_path: str = "/opt/zillow/mnist_cnn.pth",
    batch_size: int = 64,
    test_batch_size: int = 64,
    epochs: int = 1,
    optimizer: str = "sgd",
    lr: float = 0.01,
    momentum: float = 0.5,
    seed: int = 42,
    world_size: int = 2,
    rank: int = 0,
):
    from mnist_pytorch_example.models.train import train_model_io
    return train_model_io(
        input_data_path=input_data_path,
        out_path=out_path,
        batch_size=batch_size,
        test_batch_size=test_batch_size,
        epochs=epochs,
        optimizer=optimizer,
        lr=lr,
        momentum=momentum,
        seed=seed,
        world_size=world_size,
        rank=rank
    )


train_component_resources = {
    "container": {
        "set_memory_request": "1G",
        "set_memory_limit": "2G",
        # "set_gpu_limit": "1",
        "set_cpu_request": "500m",
        "set_cpu_limit": "5",
    },
    "set_retry": 2,
}

base_image: str = (
    "analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/"
    "mnist-pytorch-example:0.1.766a5007.master"
)

class HelloPyTorch(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """
    input_data_path = Parameter(
        "data_path",
        help="MNIST dataset path, local or S3",
        default="/opt/zillow/mnist_pytorch_example/data",
    )
    out_path = Parameter(
        "out_path", help="Output path to save trained model", default="/opt/zillow/mnist_cnn.pth"
    )
    batch_size = Parameter("batch_size", help="Training batch size", default=64)
    test_batch_size = Parameter(
        "test_batch_size", help="Training validation batch size", default=64
    )
    epochs = Parameter("epochs", help="Number of epochs for training", default=1)
    optimizer = Parameter("optimizer", help="Training optimizer", default="sgd")
    lr = Parameter(
        "learning_rate", help="Learning rate for gradient descent", default=0.01
    )
    momentum = Parameter("momentum", help="Momentum for gradient descent", default=0.5)
    seed = Parameter("seed", help="Init seed", default=42)
    world_size = Parameter("world_size", help="world_size", default=2)
    rank = Parameter("rank", help="rank", default=0)

    @step
    def start(self):
        """
        kfp.preceding_component_inputs Flow state ["who"] is passed to the KFP component as arguments
        """
        self.who = "world"
        self.run_id = current.run_id
        self.next(self.end)

    @kfp(
        preceding_component=graph_component(kfp_pytorch(  # TODO: kfp_pytorch bug needs graph_component
            func=my_train_model_io,
            base_image=base_image,
            component_resources=train_component_resources,
            world_size=2,
            # rank=0
        )),
        # preceding_component_inputs="input_data_path out_path batch_size test_batch_size epochs optimizer lr momentum seed world_size"
    )
    @step
    def end(self):
        """
        kfp.preceding_component_outputs ["message"] is now available as Metaflow Flow state
        """
        print("who", self.who)


if __name__ == "__main__":
    HelloPyTorch()
