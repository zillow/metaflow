# Used to create the hsezhiyan/metaflow-zillow image.
# TODO (hariharans) - move from personal account to Zillow public registry.
# The image is used in two places:
#   1. In metaflow/plugins/kfp/kfp_constants.py, where it is used as a base image
#      when the user does not provide their own base image with python flow.py kfp run --base-image...
#   2. In metaflow/plugins/kfp/kfp.py, where it is used as the base image of the 
#      s3_sensor_op.

FROM python:3.9.0

WORKDIR /opt/zillow

# Install kubectl for workflow_uid_op
RUN apt-get update && apt-get install -yq --no-install-recommends \
        apt-transport-https \
        apt-utils \
        bash \
        build-essential \
        ca-certificates \
        curl \
        dialog \
        git \
        isomd5sum \
        libffi-dev \
        libmpdec-dev \
        libssl-dev \
        sudo \
        vim \
        wget \
        zip \
        zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LO https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl \
    && sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Upgrade pip & install poetry
RUN pip install -U pip==20.0.2 poetry==1.0.10
ENV POETRY_VIRTUALENVS_CREATE=False

RUN pip install awscli click requests boto3 pytest pytest-xdist

# Create Z_USER with UID=1000 and in the 'users' group
ARG Z_USER="zservice"
ARG Z_UID="1000"
ARG Z_GID="100"

# Configure environment
ENV SHELL=/bin/bash \
    Z_USER=$Z_USER \
    Z_UID=$Z_UID \
    Z_GID=$Z_GID \
    HOME=/home/$Z_USER

# Enable users to install packages as root
RUN echo "%users ALL=(ALL) NOPASSWD:/usr/local/bin/pip" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/local/bin/pip3" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/local/bin/python" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/bin/apt-get" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/bin/apt" >> /etc/sudoers

# Copy exit_handler, get_workflow_uid, and s3_sensor code to be used
# by the Metaflow codebase.
COPY ./metaflow_container_op /opt/metaflow/metaflow_container_op
