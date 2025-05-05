from config import config
from datetime import datetime
from db import get_connection
from fallback import run_fallbacks
from rapidfuzz import fuzz
from schema import (
    transactions_parsed,
    addresses,
    match_results,
    unmatched_report
)
from sqlalchemy import select, text
import logging
import pandas as pd
import sqlalchemy


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = get_connection()

BATCH_SIZE = config.pipeline.batch_size
FUZZY_THRESHOLD = config.pipeline.fuzzy_threshold

EXACT_FIELDS = [
    (transactions_parsed.c.street_number, addresses.c.house),
    (transactions_parsed.c.street_name, addresses.c.street),
    (transactions_parsed.c.street_type, addresses.c.strtype),
    (transactions_parsed.c.unit_type, addresses.c.apttype),
    (transactions_parsed.c.unit_identifier, addresses.c.aptnbr),
    (transactions_parsed.c.city, addresses.c.city),
    (transactions_parsed.c.state, addresses.c.state),
    (transactions_parsed.c.zip, addresses.c.zip),
]


def fetch_unmatched_batch(offset):
    statement = select(transactions_parsed).offset(offset).limit(BATCH_SIZE)
    return pd.read_sql(statement, engine)

def exact_match(txn_df, addr_df):
    merged = txn_df.merge(
        addr_df,
        left_on=["street_number", "street_name", "street_type", "unit_type", "unit_identifier", "city", "state", "zip"],
        right_on=["house", "street", "strtype", "apttype", "aptnbr", "city", "state", "zip"],
        how='inner',
        suffixes=("_txn", "_addr")
    )
    merged["match_type"] = "exact"
    merged["confidence_score"] = 1.0
    merged["matched_at"] = datetime.now().isoformat()
    return merged[
        ["id_txn", "id_addr", "match_type", "confidence_score", "matched_at"]
    ].rename(columns={"id_txn": "transaction_id", "id_addr": "address_id"})

def fuzzy_match(txn_df, addr_df):
    results = []
    for _, txn in txn_df.iterrows():
        # blocking by zip, street number, and city
        candidates = addr_df[
            (addr_df.zip == txn.zip) &
            (addr_df.house == txn.street_number) &
            (addr_df.city == txn.city)
        ]
        best_score = 0
        best_match = None
        txn_street = f"{txn.street_name} {txn.street_type or ''}".strip()
        for _, addr in candidates.iterrows():
            addr_street = f"{addr.street} {addr.strtype or ''}".strip()
            score = fuzz.token_sort_ratio(txn_street, addr_street)
            if score > best_score:
                best_score = score / 100
                best_match = addr
        if best_score >= FUZZY_THRESHOLD and best_match is not None:
            # Check unit identifier
            if txn.unit_identifier and best_match.aptnbr:
                if txn.unit_identifier != best_match.aptnbr:
                    logger.debug(f"Unit mismatch: TX={txn.unit_identifier} vs {best_match.aptnbr}")
                    continue  # skip this match
            logger.debug(f"Fuzzy match found for transaction {txn.id} with address {best_match.id} with score {best_score}")
            results.append({
                "transaction_id": txn.id,
                "address_id": best_match.id,
                "match_type": "fuzzy",
                "confidence_score": best_score,
                "matched_at": datetime.now().isoformat()
        })
    return pd.DataFrame(results)

MATCHING_METHODS = {
    "exact": exact_match,
    "fuzzy": fuzzy_match,
}

def match_batch(offset=0) -> bool:
    txn_df = fetch_unmatched_batch(offset)
    if txn_df.empty:
        logger.info("No more unmatched transactions to process.")
        return False

    addr_df = pd.read_sql(select(addresses), engine)
    all_matches = []
    unmatched = txn_df.copy()
    for match_strategy in ["exact", "fuzzy"]:
        logger.info(f"Attempting {match_strategy} match...")
        match_function = MATCHING_METHODS[match_strategy]
        matches = match_function(unmatched, addr_df)
        if not matches.empty:
            logger.info(f"Inserted {len(matches)} {match_strategy} matches into match_results.")
            all_matches.append(matches)
            unmatched = unmatched[~unmatched["id"].isin(matches["transaction_id"])]
    if not unmatched.empty:
        # Attempt fallbacks
        fallback_matches = run_fallbacks(unmatched, addr_df)
        if not fallback_matches.empty:
            all_matches.append(fallback_matches)
            unmatched = unmatched[~unmatched["id"].isin(fallback_matches["transaction_id"])]
            logger.info(f"Inserted {len(fallback_matches)} fallback matches into match_results.")

    if all_matches:
        all_matches_df = pd.concat(all_matches, ignore_index=True)
        all_matches_df.to_sql(match_results.name, engine, if_exists="append", index=False)
        logger.info(f"Inserted {len(all_matches_df)} matches into match_results.")

    if not unmatched.empty:
        report = pd.DataFrame({
            "transaction_id": unmatched.id,
            "reason": "low fuzzy score",
            "attempted_at": datetime.now().isoformat()
        })
        report.to_sql("unmatched_report", engine, if_exists="append", index=False)

    return True

def match_batch_test(offset=0) -> bool:
    logger.info(f"[TEST] Matching batch at offset {offset}")
    txn_df = fetch_unmatched_batch(offset)
    logger.info(f"[TEST] Transactions fetched: {len(txn_df)}")
    if txn_df.empty:
        logger.info("[TEST] No transactions to test match on.")
        return

    addr_df = pd.read_sql(select(addresses), engine)

    logger.info(f"[TEST] Attempting exact match...")
    exact = exact_match(txn_df, addr_df)
    logger.info(f"[TEST] Exact matches: {len(exact)}")
    logger.info(f"[TEST] Unique transactions matched exactly: {exact['transaction_id'].nunique()}")
    if not exact.empty:
        logger.info("[TEST] Exact match sample:")
        logger.info("\n" + exact[[
            "transaction_id", "address_id",
            "confidence_score"
        ]].head(10).to_string(index=False))

    unmatched = txn_df[~txn_df.id.isin(exact.transaction_id)]
    logger.info(f"[TEST] Unmatched remaining: {len(unmatched)}")

    logger.info(f"[TEST] Attempting fuzzy match...")
    fuzzy = fuzzy_match(unmatched, addr_df)
    logger.info(f"[TEST] Fuzzy matches: {len(fuzzy)}")

    unmatched = unmatched[~unmatched.id.isin(fuzzy.transaction_id)]
    logger.info(f"[TEST] Unmatched remaining: {len(unmatched)}")

    logger.info(f"[TEST] Attempting fallbacks...")
    fallback = run_fallbacks(unmatched, addr_df)
    logger.info(f"[TEST] Fallback matches: {len(fallback)}")

    unmatched = unmatched[~unmatched.id.isin(fallback.transaction_id)]
    logger.info(f"[TEST] Unmatched remaining: {len(unmatched)}")

def run_match():
    offset = 0
    while match_batch(offset):
        logger.info(f"Matching batch starting at offset {offset}")
        offset += BATCH_SIZE

if __name__ == "__main__":
    run_match()
    #match_batch_test()
