
def invoke(command, shell=True):
    """
    Helper to invoke a command
    """
    import subprocess
    subprocess.call([command], shell=shell)


def perform_common_setup(code_url):
    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    invoke("curl -o helloworld.py {}".format(code_url))

    print("\n----------RUNNING: KFP Installation---------------")
    invoke("pip3 install kfp")  # Using this as a workaround until we add it to MF dependencies/our docker image

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    invoke("pip3 install --user git+https://github.com/zillow/metaflow.git@s3-integ")
