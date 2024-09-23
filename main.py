from datetime import datetime, timedelta
from data_util import fetch_data
from email_util import send_email
from dotenv import load_dotenv
import os

load_dotenv()

def main():

    record_date = os.getenv('RECORD_DATE')

    # Check if RECORD_DATE is not set or is empty
    if not record_date:
        previous_day = datetime.now() - timedelta(days=1)
        record_date = previous_day.strftime('%Y-%m-%d')


    #Making the output csv file
    fetch_data(record_date)

    #Mailing to the concerned email-ids
    send_email(record_date)
    
    
if __name__ == '__main__':
    main()
