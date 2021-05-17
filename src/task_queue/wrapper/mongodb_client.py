from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.database import Database


class MongoDBClient():
    def __init__(
        self,
        uri: str = 'mongodb://root:example@localhost/db?authSource=admin'
    ):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self._check_connection()

    def _check_connection(self):
        try:
            # The ismaster command is cheap and does not require auth.
            self.client.admin.command('ismaster')
        except ConnectionFailure:
            print("Mongodb Server not available")

    def db(self) -> Database:
        return self.client.get_default_database()
