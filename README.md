# address-matching-pipeline

## Environment Setup
* Download CSV files to `data/` folder
```bash
# inside root folder
# 1. build docker image
docker-compose build --no-cache
# 2. run docker container
docker-compose up -d
# 3. connect to docker containers
docker exec -it address-pipeline bash # address-pipeline container
docker exec -it db bash # db container

# 4. run the pipeline
# inside address-pipeline container
python src/main.py

# 5. export the results
python src/export_csv.py
```
## Architecture

### Component Overview

- **ingest.py**: Data ingestion from CSV to database
- **parse.py**: Address parsing and normalization
- **match.py**: Multi-strategy address matching
- **fallback.py**: Secondary matching strategies
- **schema.py**: Database schema definitions
- **config.py**: Configuration settings

### Database Schema

- **transactions_raw**: Raw transaction data
- **transactions_parsed**: Normalized and parsed addresses
- **addresses**: Reference address data
- **match_results**: Final matching results
- **unmatched_report**: Records of unsuccessful matches

### Matching Strategy

The pipeline employs a waterfall matching approach:

1. **Exact Matching**: First attempts to find exact matches on all address components
2. **Fuzzy Matching**: For records that don't match exactly, uses fuzzy string matching with blocking strategies
3. **Phonetic Matching**: Final fallback uses double metaphone phonetic algorithm to catch spelling variations

## Assumptions
* `id` is unique for each transaction in `11211_transactions.csv` (verified is True)
* `addresses` in `11211_addresses.csv` are normalized
* Assume all secondary address components (e.g., units, floors) represent occupancy within a building.
