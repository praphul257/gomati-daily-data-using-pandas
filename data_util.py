import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os


# Opt-in to future behavior
pd.set_option('future.no_silent_downcasting', True)

load_dotenv()

def get_db_connection():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    db_connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(db_connection_string)

def load_meter_numbers(file_path):
    meters_df = pd.read_csv(file_path)
    return [str(meter) for meter in meters_df['Meter Serial Number'].tolist()]

def fetch_blockload_data(engine, meter_numbers, start_time_str, end_time_str):
    meter_numbers_tuple = tuple(meter_numbers)
    blockload_query = f"""
    SELECT
        meter_number,
        COUNT(*) AS block_load_count
    FROM
        public.blockload_validated
    WHERE
        blockload_datetime > '{start_time_str}'
        AND blockload_datetime <= '{end_time_str}'
        AND meter_number IN {meter_numbers_tuple}
    GROUP BY
        meter_number
    """
    return pd.read_sql(blockload_query, engine)

def fetch_dailyload_data(engine, meter_numbers, end_date_str):
    meter_numbers_tuple = tuple(meter_numbers)
    dailyload_query = f"""
    SELECT DISTINCT
        device_id AS meter_number,
        1 AS daily_load
    FROM
        dailyload_validated
    WHERE
        dailyload_datetime::date = '{end_date_str}'
        AND device_id IN {meter_numbers_tuple}
    """
    return pd.read_sql(dailyload_query, engine)

def fetch_pushEvent_data(engine, meter_numbers, end_date_str):
    meter_numbers_tuple = tuple(meter_numbers)
    pushEvent_query = f"""
    SELECT COUNT(id) as push_event_count, device_id as meter_number
    FROM
        public.pushevent_validated
    WHERE
        data_timestamp::date = '{end_date_str}'
        AND device_id IN {meter_numbers_tuple}
        GROUP BY device_id
    """
    return pd.read_sql(pushEvent_query, engine)

def fetch_pullEvent_data(engine, meter_numbers, end_date_str):
    meter_numbers_tuple = tuple(meter_numbers)
    pullEvent_query = f"""
    SELECT COUNT(id) as pull_event_count, badge_number as meter_number
    FROM
        public.pullevent_validated
    WHERE
        data_timestamp::date = '{end_date_str}'
        AND badge_number IN {meter_numbers_tuple}
        GROUP BY badge_number
    """
    return pd.read_sql(pullEvent_query, engine)

def fetch_billing_data(engine, meter_numbers, start_of_month):
    meter_numbers_tuple = tuple(meter_numbers)
    monthlyBilling_query = f"""
    SELECT COUNT(id) as prev_month_billing_data, device_id as meter_number FROM public.monthlybilling_validated 
	WHERE billing_datetime ='{start_of_month}' AND device_id IN {meter_numbers_tuple}
    GROUP BY device_id
    """
    return pd.read_sql(monthlyBilling_query, engine)

def process_data(meter_numbers, blockload_df, dailyload_df, pushEvent_df, pullEvent_df, monthlyBilling_df, record_date):
    all_meters_df = pd.DataFrame({'meter_number': meter_numbers})
    #adding blockload
    merged_blockload_df = all_meters_df.merge(blockload_df, left_on='meter_number', right_on='meter_number', how='left')
    #adding dailyload
    final_df = merged_blockload_df.merge(dailyload_df, on='meter_number', how='left')
    #adding push event
    final_df = final_df.merge(pushEvent_df, on='meter_number', how='left')
    #adding pull event
    final_df = final_df.merge(pullEvent_df, on='meter_number', how='left')
    #adding monthly billing data
    final_df = final_df.merge(monthlyBilling_df, on='meter_number', how='left')
    
    #making final dataframe
    final_df['block_load_count'] = final_df['block_load_count'].fillna(0).astype(int).infer_objects(copy=False)
    final_df['daily_load'] = final_df['daily_load'].fillna(0).astype(int).infer_objects(copy=False)
    final_df['push_event_count'] = final_df['push_event_count'].fillna(0).astype(int).infer_objects(copy=False)
    final_df['pull_event_count'] = final_df['pull_event_count'].fillna(0).astype(int).infer_objects(copy=False)
    final_df['prev_month_billing_data'] = final_df['prev_month_billing_data'].fillna(0).astype(int).infer_objects(copy=False)
    final_df['record_date'] = record_date
    
    return final_df

def fetch_data(record_date):
    meter_file = os.getenv('METER_FILE')
    engine = get_db_connection()
    meter_numbers = load_meter_numbers(meter_file)
    
    record_datetime = datetime.strptime(record_date, '%Y-%m-%d')
    start_time = record_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_month = record_datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)
    current_date = end_time.strftime('%Y-%m-%d')

    blockload_df = fetch_blockload_data(engine, meter_numbers, start_time, end_time)
    dailyload_df = fetch_dailyload_data(engine, meter_numbers, current_date)
    pushEvent_df = fetch_pushEvent_data(engine, meter_numbers, current_date)
    pullEvent_df = fetch_pullEvent_data(engine, meter_numbers, current_date)
    monthlyBilling_df = fetch_billing_data(engine, meter_numbers, start_of_month)
    final_df = process_data(meter_numbers, blockload_df, dailyload_df, pushEvent_df, pullEvent_df, monthlyBilling_df, record_date)
    
    output_file = f'{record_date}_Gomati_Data.csv'
    final_df.to_csv(output_file, index=False)
    print(f"Data has been saved to {output_file}")

# Example usage:
# fetch_data('2024-08-30')
