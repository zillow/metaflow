# This image acts as both a CICD image used within the Gitlab runners run pytest which
# kicks off KFP runs using the Metaflow KFP plugin as well as the base image of Kubeflow
# pods running on the Kubeflow cluster.
FROM analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:4.0.1380
COPY . /home/zservice/metaflow
RUN pip install --upgrade --force-reinstall \
    -i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple \
    -e /home/zservice/metaflow[kfp,kfp-tests]

ARG IMAGE_TAG
ENV IMAGE_TAG=$IMAGE_TAG
