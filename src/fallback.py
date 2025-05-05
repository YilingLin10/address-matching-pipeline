from config import config
from db import get_connection
from datetime import datetime
from metaphone import doublemetaphone
from schema import transactions_parsed, addresses
import logging
import pandas as pd
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = get_connection()

def match_unit_identifier(tx_unit, addr_unit):
    if pd.isna(tx_unit) or pd.isna(addr_unit):
        return False
    norm_tx = re.sub(r"[^\w]", "", str(tx_unit)).upper()
    norm_addr = re.sub(r"[^\w]", "", str(addr_unit)).upper()
    return norm_tx == norm_addr

def phonetic_fallback(txn_df, addr_df):
    results = []
    for _, txn in txn_df.iterrows():
        # compute the phonetic code for the street name
        txn_code = doublemetaphone(txn.street_name)[0]
        # blocking on zip code
        candidates = addr_df[(addr_df.house == txn.street_number)]

        for _, addr in candidates.iterrows():
            addr_code = doublemetaphone(addr.street)[0]
            if txn_code == addr_code:
                if txn.unit_identifier and addr.aptnbr:
                    if not match_unit_identifier(txn.unit_identifier, addr.aptnbr):
                        logger.debug(f"Unit mismatch: TXN={txn.unit_identifier} vs ADDR={addr.aptnbr}")
                logger.debug(f"Phonetic match found: {txn.street_name} -> {addr.street}")
                results.append({
                    "transaction_id": txn.id,
                    "address_id": addr.id,
                    "match_type": "phonetic",
                    "confidence_score": 0.6,  # heuristic
                    "matched_at": datetime.now().isoformat()
                })
                if config.pipeline.take_first_phonetic_match:
                    break
    return pd.DataFrame(results)

def run_fallbacks(unmatched_df, addr_df):
    logger.info(f"[FALLBACK] Starting fallback with {len(unmatched_df)} unmatched transactions")

    phonetic_df = phonetic_fallback(unmatched_df, addr_df)
    logger.info(f"[FALLBACK] Phonetic matches: {len(phonetic_df)}")

    return phonetic_df

