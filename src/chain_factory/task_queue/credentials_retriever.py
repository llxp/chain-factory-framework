from json import dumps

from requests import post, get

from api.routes.v1.models.credentials import ManagementCredentials


class CredentialsRetriever():
    """
    Retrieve database credentials from rest api
    """

    def __init__(
        self,
        endpoint: str,
        namespace: str,
        username: str,
        password: str,
        key: str
    ):
        self.endpoint = endpoint
        self.namespace = namespace
        self.username = username
        self.password = password
        self.mongodb_host = 'localhost'
        self.mongodb_port = 27017
        self.headers = dict(
            Accept='application/json',
            ContentType='application/json'
        )
        self.extra_arguments = dict(
            authSource='admin'
        )
        self.key = key
        self.credentials = None

    async def init(self):
        self.jwe_token = self.get_jwe_token(self.username, self.password)
        self.credentials = self.get_credentials()

    @property
    def mongodb(self) -> str:
        # get db credentials from credentials
        return self.credentials.credentials.mongodb.url

    @property
    def redis(self) -> str:
        # get redis credentials from credentials
        return self.credentials.credentials.redis.url

    @property
    def rabbitmq(self) -> str:
        # get rabbitmq credentials from credentials
        return self.credentials.credentials.rabbitmq.url

    def get_jwe_token(self, username, password):
        # send login request to /api/login
        response = post(
            url=self.endpoint + '/api/login',
            data=dumps({
                'username': username,
                'password': password
            }),
            headers=self.headers
        )
        # get jwe token from response
        return response.json()

    def get_credentials(self) -> ManagementCredentials:
        headers = {
            'Authorization': 'Bearer {}'.format(self.jwe_token),
            'Content-Type': 'application/json'
        }
        # send request to /api/orchestrator/credentials
        response = get(
            url='{}/api/orchestrator/credentials?namespace={}&key={}'.format(
                self.endpoint, self.namespace, self.key
            ),
            headers=headers
        )
        if response.status_code == 200:
            # get credentials from response
            return ManagementCredentials(**response.json())
        return None
