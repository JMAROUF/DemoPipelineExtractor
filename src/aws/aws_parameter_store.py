from src.aws.AWSBase import AWSBase


class AwsParameterStore(AWSBase):
    def __init__(self,profile_name=None):
        self.client_ssm=super()._create_basic_session(profile_name).client('ssm')

    def get_secret_parameter(self,parameter_name:str):
        response=self.client_ssm.get_parameter(Name=parameter_name,WithDecryption=True)
        return response['Parameter']['Value']