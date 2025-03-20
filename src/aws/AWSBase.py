import os

import boto3.session


class AWSBase:
    @staticmethod
    def _create_basic_session(profile_name=None):
        if os.getenv("PROXY_USAGE") is not None:
            return boto3.session.Session(profile_name=profile_name)
        return boto3.session.Session()

    @staticmethod
    def _get_glue(profile_name=None):
        session=AWSBase._create_basic_session(profile_name=None)
        return session.client('glue')