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
        self._credentials: Dict[CredentialsRetriever] = {}
        for namespace in namespaces:
            self.get_credentials(namespace)

    def get_credentials(self, namespace: str) -> CredentialsRetriever:
        """
        Get the credentials for the namespace.
        """
        try:
            return self._credentials[namespace]
        except KeyError:
            self._credentials[namespace] = CredentialsRetriever(
                self.endpoint, namespace, self.username, self.password)
            return self._credentials[namespace]
