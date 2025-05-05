from config import config
from sqlalchemy import create_engine

def get_connection():
    return create_engine(config.db.url)
