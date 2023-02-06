set -exo
python3 etl_web_to_gcs.py --year 2019 --month 2 3 --color yellow
python3 etl_gcs_to_bq.py --year 2019 --month 2 3 --color yellow
