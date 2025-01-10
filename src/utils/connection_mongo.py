
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import yaml

with open("../../../../conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

uri = f"mongodb+srv://{config.mongo_user}:{config.mongo_password}@cluster0.y1qgl.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(uri, server_api=ServerApi('1'))


def get_connection():
    return client
