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
## Assumptions
* `id` is unique for each transaction in `11211_transactions.csv` (verified is True)
* `addresses` in `11211_addresses.csv` are normalized
* We assume all secondary address components (e.g., units, floors) represent occupancy within a building.
