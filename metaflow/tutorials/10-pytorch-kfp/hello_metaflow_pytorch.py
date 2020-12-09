from metaflow import FlowSpec, Parameter, step, current, resources


class HelloMetaflowPyTorch(FlowSpec):
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

    @step
    def start(self):
        """
        """
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @resources(
        cpu=1, cpu_limit=2,
        memory="2G", memory_limit="5G"
    )
    @step
    def train(self):
        """
        PyTorch train step
        """
        from mnist_pytorch_example.models.train import train_model_io

        self.rank = self.input
        print("self.rank", self.rank)
        out_path = train_model_io(  # TODO: not call it _io, can return the model
            input_data_path=self.input_data_path,
            out_path=self.out_path,
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

        if self.rank == 0:
            import torch
            self.model = torch.load(out_path)
            self.model_path = out_path  # TODO: don't need this, it's returned by train_model()

        self.next(self.evaluate)


    @step
    def evaluate(self, inputs):
        train_input = next((x for x in inputs if x.rank == 0), None)

        print(train_input)
        self.model = train_input.model
        self.model_path = train_input.model_path

        # Evaluate!
        import torch
        from mnist_pytorch_example.models.evaluate import evaluate_io

        torch.save(self.model.state_dict(), self.model_path)  # TODO: update evalauate_io
        self.evaluate_results = evaluate_io(
            model_state_dict_path=self.model_path,
            input_data_path=self.input_data_path,
            batch_size=self.batch_size,
            test_batch_size=self.test_batch_size,
            test_accuracy_threshold=0.1,
            train_accuracy_threshold=0.1,
        )

        self.next(self.end)

    @step
    def end(self):
        """
        kfp.preceding_component_outputs ["message"] is now available as Metaflow Flow state
        """
        print(f"model: {self.model}")
        print(f"evaluate_results: {self.evaluate_results}")


if __name__ == "__main__":
    HelloMetaflowPyTorch()
