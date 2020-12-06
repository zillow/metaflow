from aip_kfp_sdk.components.pytorch import kfp_pytorch
from kfp.dsl import graph_component

from metaflow import FlowSpec, Parameter, step, kfp, current


# TODO: wrapping train_model_io, to load it at runtime within the container
#  once, the mnist-example project doit debug image works to submit a pipeline
#  then my_train_model_io() can be removed.
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
        "set_cpu_request": "500m",
        "set_cpu_limit": "5",
    },
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

    # TODO: decorators don't have access to Metaflow parameters
    #  but could they have access to the CLI arguments somehow?
    #  Also, how could world_size, then also be a pipeline argument anymore?
    #  Only if kfp_pytorch were a graph_component that accepts (world_size, base_image)
    #  as a parameter.
    @kfp(
        # TODO: kfp_pytorch bug needs graph_component, else dependency graph is off!
        preceding_component=graph_component(
            kfp_pytorch(
                func=my_train_model_io,
                base_image=base_image,
                component_resources=train_component_resources,
                world_size=2,
            )
        ),
        # TODO: kfp_pytorch when world_size=2 doesn't accept following kfp parameters!
        #  world_size=1 however works and accepts the parameters with this change:
        #    diff --git a/aip_kfp_sdk/components/pytorch.py b/aip_kfp_sdk/components/pytorch.py
        #    index 28dd32c..1a221ca 100644
        #    --- a/aip_kfp_sdk/components/pytorch.py
        #    +++ b/aip_kfp_sdk/components/pytorch.py
        #    @@ -137,7 +137,7 @@ def kfp_pytorch(
        #             else:
        #                 return kfp_component(
        #                     func, base_image=base_image, component_resources=component_resources
        #    -            )(**wrapper_kwargs)
        #    +            )(*args, **wrapper_kwargs)
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
