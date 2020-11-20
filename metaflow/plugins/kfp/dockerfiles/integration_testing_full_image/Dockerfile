FROM analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py36-cpu:2.3.2824930b.alexla-aip-2704
COPY . /home/zservice/metaflow
RUN pip install --upgrade --no-deps --force-reinstall -e /home/zservice/metaflow
RUN pip install pytest==6.1.2 pytest-xdist==2.1.0 click==7.1.2 requests==2.24.0 boto3==1.15.6
