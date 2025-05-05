from easydict import EasyDict as edict
import os

config = edict()

config.db = edict()
config.db.url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/addresses")

config.paths = edict()
config.paths.transactions_raw = "./data/transactions_2_11211.csv"
config.paths.addresses = "./data/11211_Addresses.csv"

config.pipeline = edict()
config.pipeline.batch_size = 1000  # Number of records to process in each batch
config.pipeline.match_strategy = "fuzzy"  # Options: "exact", "fuzzy"
config.pipeline.fuzzy_threshold = 0.8  # Threshold for fuzzy matching
config.pipeline.take_first_phonetic_match = True # If True, take the first phonetic match if False, all matches are included

config.output = edict()
config.output.dir = "./output"
config.output.matches_csv = "matches.csv"
config.output.unmatched_csv = "unmatched.csv"

