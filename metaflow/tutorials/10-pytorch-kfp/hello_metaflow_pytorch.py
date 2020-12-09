from metaflow import FlowSpec, Parameter, step, pytorch, resources


class HelloMetaflowPyTorch(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """
    input_data_path = Parameter(
        "data_path",
        help="MNIST dataset path, local or S3",
        default="/opt/zillow/mnist_pytorch_example/data",
    )
    model_path = Parameter(
        "model_path", help="Output path to save trained model", default="/opt/zillow/mnist_cnn.pth"
    )
    batch_size = Parameter("batch_size", help="Training batch size", default=1000)
    test_batch_size = Parameter(
        "test_batch_size", help="Training validation batch size", default=1000
    )
    epochs = Parameter("epochs", help="Number of epochs for training", default=1)
    optimizer = Parameter("optimizer", help="Training optimizer", default="sgd")
    lr = Parameter(
        "learning_rate", help="Learning rate for gradient descent", default=0.01
    )
    momentum = Parameter("momentum", help="Momentum for gradient descent", default=0.5)
    seed = Parameter("seed", help="Init seed", default=42)
    world_size = Parameter("world_size", help="world_size", default=1)
    train_accuracy_threshold = Parameter(
        "train_accuracy_threshold",
        help="Training dataset accuracy threshold",
        default=0.5
    )
    test_accuracy_threshold = Parameter(
        "test_accuracy_threshold",
        help="Test dataset accuracy threshold",
        default=0.5
    )

    @step
    def start(self):
        """
        Initialize the world_size ranks for PyTorch trainers
        """
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @resources(
        cpu=1, cpu_limit=2,
        memory="2G", memory_limit="5G"
    )
    @pytorch(shared_volume_dir="/opt/zillow/shared/")  # TODO: train_model should use the default
    @step
    def train(self):
        """
        PyTorch train step
        """
        self.rank = self.input
        print("self.rank", self.rank)

        from mnist_pytorch_example.models.train import train_model
        self.model_state_dict = train_model(
            input_data_path=self.input_data_path,
            model_path=self.model_path,
            batch_size=self.batch_size,
            test_batch_size=self.test_batch_size,
            epochs=self.epochs,
            optimizer=self.optimizer,
            lr=self.lr,
            momentum=self.momentum,
            seed=self.seed,
            world_size=self.world_size,
            rank=self.rank
        )

        self.next(self.evaluate)


    @step
    def evaluate(self, inputs):
        train_input = next((x for x in inputs if x.rank == 0), None)

        print("train_input", train_input)
        self.model_state_dict = train_input.model_state_dict

        from mnist_pytorch_example.models.evaluate import evaluate_model
        self.evaluate_results = evaluate_model(
            model_state_dict=self.model_state_dict,
            input_data_path=self.input_data_path,
            batch_size=self.batch_size,
            test_batch_size=self.test_batch_size,
            train_accuracy_threshold=self.train_accuracy_threshold,
            test_accuracy_threshold=self.test_accuracy_threshold
        )

        self.next(self.end)

    @step
    def end(self):
        """
        Done! Can now publish or validate the results.
        """
        print(f"model: {self.model_state_dict}")
        print(f"evaluate_results: {self.evaluate_results}")


if __name__ == "__main__":
    HelloMetaflowPyTorch()
