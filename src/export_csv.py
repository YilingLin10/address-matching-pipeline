from config import config
from db import get_connection
import logging
import os
import pandas as pd


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = get_connection()

def export_final_matches():
    df = pd.read_sql_table("match_results", engine)
    output_path = os.path.join(config.output.dir, config.output.matches_csv)
    df.to_csv(output_path, index=False)
    logger.info(f"Exported final match results to {output_path}")

def export_unmatched_report():
    df = pd.read_sql_table("unmatched_report", engine)
    output_path = os.path.join(config.output.dir, config.output.unmatched_csv)
    df.to_csv(output_path, index=False)
    logger.info(f"Exported unmatched report to {output_path}")

if __name__ == "__main__":
    export_final_matches()
    export_unmatched_report()
