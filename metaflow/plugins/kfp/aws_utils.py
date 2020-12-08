"""
TODO: Merge this file with metaflow/plugins/aws/aws_client.py

"""

import boto3
from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    DeferredRefreshableCredentials,
)
from botocore.session import Session
from datetime import datetime
from dateutil.tz import tzlocal


def _assumed_role_session(role_arn: str = None) -> boto3.session.Session:
    """
    Get a boto3 session with assumed role and auto-refresh
    Args:
        role_arn: Assume role account

    Returns: boto3 session

    """
    if role_arn is None:  # running in pipeline, role already assumed
        return boto3.Session()

    source_session = boto3.Session()
    # Use profile to fetch assume role credentials
    fetcher = AssumeRoleCredentialFetcher(
        client_creator=source_session._session.create_client,
        source_credentials=source_session.get_credentials(),
        role_arn=role_arn,
    )
    # Create new session with assumed role and auto-refresh
    botocore_session = Session()
    botocore_session._credentials = DeferredRefreshableCredentials(
        method="assume-role",
        refresh_using=fetcher.fetch_credentials,
        time_fetcher=lambda: datetime.now(tzlocal()),
    )
    return boto3.Session(botocore_session=botocore_session, region_name="us-west-2")


def get_aws_client(
    role_arn: str = None, service: str = "s3"
) -> boto3.session.Session.client:
    session = _assumed_role_session(role_arn)
    return session.client(service)