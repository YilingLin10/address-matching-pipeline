from config import config
from ingest import load_data
from parse import normalize_and_parse
from match import run_match

if __name__ == "__main__":
    load_data()
    normalize_and_parse()
    run_match()
