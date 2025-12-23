from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import logging
from urllib.request import urlopen, Request

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'egx30_stock_pipeline',
    default_args=default_args,
    description='Daily EGX30 stock data pipeline',
    schedule_interval='0 18 * * *',
    catchup=False
)

def extract_stock_data(**context):
    """Extract Egyptian stock data from Yahoo Finance"""
    logging.info("Starting data extraction...")
    
    execution_date = context['ds']
    
    # EGX stock symbols
    egx_symbols = [
        'EGS01041C010.CA', 'EGS01071C017.CA', 'EGS01081C016.CA',
        'EGS02021C011.CA', 'EGS02051C018.CA', 'EGS02091C014.CA',
        'EGS02211C018.CA', 'EGS02291C010.CA', 'EGS07061C012.CA'
    ]
    
    all_data = []
    
    for symbol in egx_symbols:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            req = Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0')
            
            with urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
                result = data.get('chart', {}).get('result', [])
                
                if result:
                    quote = result[0].get('indicators', {}).get('quote', [{}])[0]
                    close_prices = quote.get('close', [])
                    
                    if close_prices and close_prices[0] is not None:
                        price = round(close_prices[0], 2)
                        clean_symbol = symbol.replace('.CA', '')
                        all_data.append({
                            'date': execution_date,
                            'symbol': clean_symbol,
                            'price': price
                        })
                        logging.info(f"Extracted {clean_symbol}: {price}")
        except Exception as e:
            logging.error(f"Error for {symbol}: {str(e)}")
    
    if not all_data:
        raise ValueError("No data extracted")
    
    # Save CSV
    csv_path = f'/tmp/egx30_{execution_date}.csv'
    with open(csv_path, 'w') as f:
        f.write('date,stock_symbol,price\n')
        for row in all_data:
            f.write(f"{row['date']},{row['symbol']},{row['price']}\n")
    
    logging.info(f"Saved {len(all_data)} records")
    return csv_path

def validate_and_prepare(**context):
    """Validate and prepare data for Hive"""
    ti = context['ti']
    csv_path = ti.xcom_pull(task_ids='extract_data')
    execution_date = context['ds']
    
    # Read and validate
    data = []
    with open(csv_path, 'r') as f:
        f.readline()  # Skip header
        for line in f:
            parts = line.strip().split(',')
            if len(parts) == 3:
                try:
                    data.append({
                        'symbol': parts[1],
                        'price': float(parts[2])
                    })
                except ValueError:
                    continue
    
    # Remove duplicates
    seen = set()
    unique_data = []
    for row in data:
        if row['symbol'] not in seen:
            seen.add(row['symbol'])
            unique_data.append(row)
    
    logging.info(f"Validated {len(unique_data)} unique records")
    
    # Create Hive data file (without header)
    hive_data = f'/tmp/hive_data_{execution_date}.txt'
    with open(hive_data, 'w') as f:
        for row in unique_data:
            f.write(f"{row['symbol']},{row['price']}\n")
    
    # Create HQL script
    hql_script = f'/tmp/load_hive_{execution_date}.hql'
    with open(hql_script, 'w') as f:
        f.write(f"""
-- Step 1: Create staging table (TEXT format for CSV)
CREATE TABLE IF NOT EXISTS egx30_staging (
    stock_symbol STRING,
    price DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Step 2: Create final ORC table (partitioned)
CREATE TABLE IF NOT EXISTS egx30_stocks (
    stock_symbol STRING,
    price DECIMAL(10,2)
)
PARTITIONED BY (trade_date STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- Step 3: Clear staging
TRUNCATE TABLE egx30_staging;

-- Step 4: Load CSV into staging
LOAD DATA LOCAL INPATH '{hive_data}' INTO TABLE egx30_staging;

-- Step 5: Drop old partition
ALTER TABLE egx30_stocks DROP IF EXISTS PARTITION (trade_date='{execution_date}');

-- Step 6: Insert from staging to final table
INSERT INTO TABLE egx30_stocks PARTITION (trade_date='{execution_date}')
SELECT stock_symbol, price FROM egx30_staging;

-- Step 7: Verify
SELECT trade_date, COUNT(*) as record_count 
FROM egx30_stocks 
WHERE trade_date='{execution_date}'
GROUP BY trade_date;
""")
    
    logging.info(f"Created HQL script: {hql_script}")
    return hql_script

# Tasks
extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_stock_data,
    dag=dag
)

validate = PythonOperator(
    task_id='validate_and_prepare',
    python_callable=validate_and_prepare,
    dag=dag
)

load = BashOperator(
    task_id='load_to_hive',
    bash_command='hive -f {{ ti.xcom_pull(task_ids="validate_and_prepare") }}',
    dag=dag
)

# Flow
extract >> validate >> load