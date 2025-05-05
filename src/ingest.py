from config import config
from db import get_connection
import logging
import pandas as pd
import sqlalchemy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
engine = get_connection()

def ingest_address(csv_path):
    df = pd.read_csv(csv_path)
    df = df.rename(columns={"address": "full_address"})
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("TRUNCATE TABLE addresses CASCADE;"))
    df.to_sql(
        "addresses",
        engine,
        if_exists="append",
        index=False
    )
    logger.info(f"Inserted {len(df)} rows into addresses table.")

def ingest_transactions(csv_path):
    df = pd.read_csv(csv_path)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("TRUNCATE TABLE transactions_raw CASCADE;"))
    df.to_sql(
        "transactions_raw",
        engine,
        if_exists="append",
        index=False
    )
    logger.info(f"Inserted {len(df)} rows into transactions_raw table.")

def load_data():
    """
    Load data from CSV files into the database.
    """
    try:
        logger.info("Loading addresses...")
        ingest_address(config.paths.addresses)
        logger.info("Loading transactions...")
        ingest_transactions(config.paths.transactions_raw)
    except sqlalchemy.exc.IntegrityError as e:
        logger.error(f"Integrity error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    else:
        logger.info("Data loaded successfully.")

if __name__ == "__main__":
    load_data()
