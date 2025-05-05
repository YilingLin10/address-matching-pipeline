DROP TABLE IF EXISTS unmatched_report CASCADE;
DROP TABLE IF EXISTS match_results CASCADE;
DROP TABLE IF EXISTS transactions_parsed CASCADE;
DROP TABLE IF EXISTS transactions_raw CASCADE;
DROP TABLE IF EXISTS addresses CASCADE;

CREATE TABLE addresses (
  id SERIAL PRIMARY KEY,
  hhid TEXT,
  fname TEXT,
  mname TEXT,
  lname TEXT,
  suffix TEXT,
  full_address TEXT,
  house TEXT,
  predir TEXT,
  street TEXT,
  strtype TEXT,
  postdir TEXT,
  apttype TEXT,
  aptnbr TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  homeownercd TEXT
);

CREATE TABLE transactions_raw (
  id TEXT PRIMARY KEY,
  status TEXT,
  price INTEGER,
  bedrooms INTEGER,
  bathrooms INTEGER,
  square_feet INTEGER,
  address_line_1 TEXT,
  address_line_2 TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  property_type TEXT,
  year_built TEXT,
  presented_by TEXT,
  brokered_by TEXT,
  presented_by_mobile TEXT,
  mls TEXT,
  listing_office_id TEXT,
  listing_agent_id TEXT,
  created_at TEXT,
  updated_at TEXT,
  open_house TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  email TEXT,
  list_date TEXT,
  pending_date TEXT,
  presented_by_first_name TEXT,
  presented_by_last_name TEXT,
  presented_by_middle_name TEXT,
  presented_by_suffix TEXT,
  geog TEXT
);

CREATE TABLE transactions_parsed (
  id TEXT PRIMARY KEY REFERENCES transactions_raw(id),
  street_number TEXT,
  street_name TEXT,
  street_type TEXT,
  street_predir TEXT,
  street_postdir TEXT,
  unit_type TEXT,
  unit_identifier TEXT,
  city TEXT,
  state TEXT,
  zip TEXT
);

CREATE TABLE match_results (
  id SERIAL PRIMARY KEY,
  transaction_id TEXT REFERENCES transactions_parsed(id),
  address_id INTEGER REFERENCES addresses(id),
  match_type TEXT,
  confidence_score FLOAT,
  matched_at TEXT
);

CREATE TABLE unmatched_report (
  transaction_id TEXT REFERENCES transactions_raw(id),
  reason TEXT,
  attempted_at TEXT
);

CREATE INDEX idx_transactions_parsed_address ON transactions_parsed (
  street_number, street_name, street_type, street_predir, street_postdir, unit_type, unit_identifier, city, state, zip
);

CREATE INDEX idx_addresses_address ON addresses (
  house, street, strtype, predir, postdir, apttype, aptnbr, city, state, zip
);

CREATE INDEX idx_match_transaction_id ON match_results(transaction_id);
