import json
import requests


class CredentialsRetriever():
    """
    Retrieve database credentials from rest api
    """

    def __init__(
        self,
        endpoint: str,
        namespace: str,
        username: str,
        password: str
    ):
        self.endpoint = endpoint
        self.namespace = namespace
        self.jwe_token = self.get_jwe_token(username, password)
        self.credentials = self.extract_credentials(self.get_credentials())

    def extract_credentials(self, credentials):
        # get credentials from credentials
        if 'credentials' in credentials:
            return credentials['credentials']
        return None

    def mongodb(self):
        # get db credentials from credentials
        return self.credentials['mongodb']

    def redis(self):
        # get redis credentials from credentials
        return self.credentials['redis']

    def rabbitmq(self):
        # get rabbitmq credentials from credentials
        return self.credentials['rabbitmq']

    def get_jwe_token(self, username, password):
        # send login request to /api/login
        response = requests.post(
            url=self.endpoint + '/api/login',
            data=json.dumps({
                'username': username,
                'password': password
            }),
            headers=self.headers
        )
        # get jwe token from response
        return response.json()

    def get_credentials(self):
        headers = {
            'Authorization': 'Bearer {}'.format(self.jwe_token),
            'Content-Type': 'application/json'
        }
        # send request to /api/orchestrator/credentials
        response = requests.get(
            url='{}/api/orchestrator/credentials?namespace={}&key={}'.format(
                self.url, self.namespace, self.key
            ),
            headers=headers
        )
        # get credentials from response
        return response.json()
