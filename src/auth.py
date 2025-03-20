import boto3
import requests

# URL de l'API externe pour obtenir les identités temporaires
api_url = 'https://example.com/get-temporary-credentials'

# Clé API
api_key = 'votre_api_key'

# En-têtes de la requête
headers = {
    'Authorization': f'Bearer {api_key}'
}

# Faire la requête pour obtenir les identités temporaires
response = requests.get(api_url, headers=headers)
credentials = response.json()

# Configurer les identités temporaires dans boto3
session = boto3.Session(
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

# Utiliser les identités temporaires pour accéder aux services AWS
client = session.client('ssm')
response = client.get_parameter(Name='nom_du_parametre')
print(response['Parameter']['Value'])