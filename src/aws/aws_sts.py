import boto3


class AwsSts:
    def __init__(self):
        self.client = boto3.client('sts')
        self.assumed_role = None

    def assume_role(self, role_arn, role_session_name):
        self.assumed_role = self.client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name
        )
        return self.assumed_role

    def get_credentials(self):
        return self.assumed_role['Credentials']