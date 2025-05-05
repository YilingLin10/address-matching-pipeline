from config import config
from datetime import datetime
from db import get_connection
from exception import InvalidAddressTypeError
from normalize import normalize_tagged_address, NormalizationError
from schema import transactions_raw, transactions_parsed, unmatched_report
from sqlalchemy import select
import logging
import pandas as pd
import usaddress

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = get_connection()

STREET_ADDRESS_TYPE = "Street Address"

def postprocess_place_name(tagged):
    """Fix PlaceName that includes unit info like 'Unit PHA, Brooklyn'."""
    place = tagged.get("PlaceName", "")
    if "Unit" in place and "," in place:
        unit_part, city_part = map(str.strip, place.split(",", 1))
        if unit_part.upper().startswith("UNIT"):
            tagged["OccupancyType"] = "Unit"
            tagged["OccupancyIdentifier"] = unit_part.split(" ", 1)[-1]
            tagged["PlaceName"] = city_part
    return tagged

def normalize_and_parse(batch_size=config.pipeline.batch_size):
    offset = 0
    total_inserted = 0
    unmatched_records = []

    while True:
        statement = (
            select(
                transactions_raw.c.id,
                transactions_raw.c.address_line_1,
                transactions_raw.c.address_line_2,
                transactions_raw.c.city,
                transactions_raw.c.state,
                transactions_raw.c.zip_code,
            )
            # skip already parsed transactions
            .where(~transactions_raw.c.id.in_(select(transactions_parsed.c.id)))
            .limit(batch_size)
            .offset(offset)
        )
        df = pd.read_sql(statement, engine)
        if df.empty:
            break
        logger.info(f"Processing batch with offset {offset} and size {len(df)}")

        parsed_rows = []
        for _, row in df.iterrows():
            address = (
                f"{row['address_line_1']}"
                f"{', ' + row['address_line_2'] if row['address_line_2'] else ''}, "
                f"{row['city']} {row['state']} {row['zip_code']}"
            )
            try:
                tagged, address_type = usaddress.tag(address)
                if address_type != STREET_ADDRESS_TYPE:
                    raise InvalidAddressTypeError("Invalid address type")
                tagged = postprocess_place_name(tagged)
                normalized = normalize_tagged_address(tagged)
                normalized["id"] = row["id"]
                parsed_rows.append(normalized)
            except (
                usaddress.RepeatedLabelError,
                InvalidAddressTypeError,
                NormalizationError
            ) as e:
                logger.warning(f"Could not parse address {address}: {e}")
                unmatched_records.append({
                    "transaction_id": row["id"],
                    "reason": str(e),
                    "attempted_at": datetime.now().isoformat()
                })

        if parsed_rows:
            parsed_df = pd.DataFrame(parsed_rows)
            parsed_df.to_sql("transactions_parsed", engine, if_exists="append", index=False)
            total_inserted += len(parsed_df)
            logger.info(f"Inserted {len(parsed_df)} rows into transactions_parsed")

        if unmatched_records:
            unmatched_df = pd.DataFrame(unmatched_records)
            unmatched_df.to_sql("unmatched_report", engine, if_exists="append", index=False)
            logger.info(f"Inserted {len(unmatched_df)} rows into unmatched_report")
            unmatched_records.clear()

        offset += batch_size

    logger.info(f"Total inserted rows: {total_inserted} in transactions_parsed")

def test_sample_parsing(sample_size=300):
    statement = (
        select(
            transactions_raw.c.id,
            transactions_raw.c.address_line_1,
            transactions_raw.c.address_line_2,
            transactions_raw.c.city,
            transactions_raw.c.state,
            transactions_raw.c.zip_code,
        )
    )
    df = pd.read_sql(statement, engine)

    if df.empty:
        logger.info("No records found for testing.")
        return

    sample_df = df.sample(n=sample_size, random_state=42)
    unmatched_records = []
    for _, row in sample_df.iterrows():
        address = (
            f"{row['address_line_1']}"
            f"{', ' + row['address_line_2'] if row['address_line_2'] else ''}, "
            f"{row['city']} {row['state']} {row['zip_code']}"
        )
        try:
            tagged, address_type = usaddress.tag(address)
            if address_type != STREET_ADDRESS_TYPE:
                raise InvalidAddressTypeError(f"Invalid address type {address_type}")
            tagged = postprocess_place_name(tagged)
            normalized = normalize_tagged_address(tagged)
            normalized["id"] = row["id"]
            logger.info(f"\nOriginal: {address}\nTagged: {tagged}\nNormalized: {normalized}\n")
        except (
            usaddress.RepeatedLabelError,
            InvalidAddressTypeError,
            NormalizationError
        ) as e:
            unmatched_records.append({
                "transaction_id": row["id"],
                "reason": 'failed parse',
                "address": address,
            })

    for r in unmatched_records:
        logger.warning(f"Could not parse transaction {r['transaction_id']} - address {r['address']}: {r['reason']}")

if __name__ == "__main__":
    normalize_and_parse()
    #test_sample_parsing()
