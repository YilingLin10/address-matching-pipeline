from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Float,
    MetaData,
    ForeignKey
)

metadata = MetaData()

transactions_raw = Table(
    "transactions_raw", metadata,
    Column("id", String, primary_key=True),
    Column("status", String),
    Column("price", Integer),
    Column("bedrooms", Integer),
    Column("bathrooms", Integer),
    Column("square_feet", Integer),
    Column("address_line_1", String),
    Column("address_line_2", String),
    Column("city", String),
    Column("state", String),
    Column("zip_code", String),
    Column("property_type", String),
    Column("year_built", String),
    Column("presented_by", String),
    Column("brokered_by", String),
    Column("presented_by_mobile", String),
    Column("mls", String),
    Column("listing_office_id", String),
    Column("listing_agent_id", String),
    Column("created_at", String),
    Column("updated_at", String),
    Column("open_house", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("email", String),
    Column("list_date", String),
    Column("pending_date", String),
    Column("presented_by_first_name", String),
    Column("presented_by_last_name", String),
    Column("presented_by_middle_name", String),
    Column("presented_by_suffix", String),
    Column("geog", String),
)

transactions_parsed = Table(
    "transactions_parsed", metadata,
    Column("id", String, ForeignKey("transactions_raw.id"), primary_key=True),
    Column("street_number", String),
    Column("street_name", String),
    Column("street_type", String),
    Column("street_predir", String),
    Column("street_postdir", String),
    Column("unit_type", String),
    Column("unit_identifier", String),
    Column("city", String),
    Column("state", String),
    Column("zip", String),
)

addresses = Table(
    "addresses", metadata,
    Column("id", Integer, primary_key=True),
    Column("hhid", String),
    Column("fname", String),
    Column("mname", String),
    Column("lname", String),
    Column("suffix", String),
    Column("full_address", String),
    Column("house", String),
    Column("predir", String),
    Column("street", String),
    Column("strtype", String),
    Column("postdir", String),
    Column("apttype", String),
    Column("aptnbr", String),
    Column("city", String),
    Column("state", String),
    Column("zip", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("homeownercd", String),
)

match_results = Table(
    "match_results", metadata,
    Column("id", Integer, primary_key=True),
    Column("transaction_id", String, ForeignKey("transactions_parsed.id")),
    Column("address_id", Integer, ForeignKey("addresses.id")),
    Column("match_type", String),
    Column("confidence_score", Float),
    Column("matched_at", String),
)

unmatched_report = Table(
    "unmatched_report", metadata,
    Column("transaction_id", Integer, ForeignKey("transactions_raw.id")),
    Column("reason", String),
    Column("attempted_at", String),
)

