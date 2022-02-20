from typing import Dict, List
from .credentials_retriever import CredentialsRetriever


class CredentialsPool:
    """
    CredentialsPool is a class that is responsible for managing the credentials
    for a specific namespace.
    """

    def __init__(
        self,
        endpoint: str,
        username: str,
        password: str,
        namespaces: List[str] = []
    ):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.namespaces = namespaces
        self._credentials: Dict[str, CredentialsRetriever] = {}

    async def init(self):
        for namespace in self.namespaces:
            self.get_credentials(namespace)

    async def get_credentials(
        self,
        namespace: str,
        key: str
    ) -> CredentialsRetriever:
        """
        Get the credentials for the namespace.
        """
        try:
            return self._credentials[namespace]
        except KeyError:
            self._credentials[namespace] = CredentialsRetriever(
                self.endpoint, namespace, self.username, self.password, key)
            await self._credentials[namespace].init()
            return self._credentials[namespace]

    async def update_credentials(self):
        """
        Update the credentials for the namespace.
        """
        namespaces = self._credentials.keys()
        self._credentials = {}
        for namespace in namespaces:
            await self.get_credentials(namespace)
