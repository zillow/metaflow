def pytest_addoption(parser):
    """
    Corresponds to a command line argument for the image tag of
    the hsezhiyan/kfp-base image on Dockerhub that corresponds to the
    Metaflow version of the current commit.
    """
    parser.addoption("--tag", action="store", default="default_tag")
