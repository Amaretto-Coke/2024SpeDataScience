import requests
import csv
from datetime import datetime as dt

# Read the AESO API key from a file
with open('aeso-key.txt', 'r') as f:
    aeso_key = f.read().strip()


# Function to save data to CSV
def save_to_csv(response_data, response_year):
    # Define the CSV file name based on the year
    filename = rf'AesoPoolPriceData\aeso_pool_price_{response_year}.csv'

    # Define the CSV column names
    csv_headers = ['begin_datetime_utc', 'begin_datetime_mpt', 'pool_price', 'forecast_pool_price', 'rolling_30day_avg']

    # Write data to CSV
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=csv_headers)
        writer.writeheader()
        for entry in response_data:
            writer.writerow(entry)
    print(f'Saved data for {response_year} to {filename}')


# The main execution block
if __name__ == '__main__':
    today = dt.today()
    for year in range(2000, today.year + 1):
        start_date = dt(year, 1, 1).strftime('%Y-%m-%d')
        end_date = dt(year, 12, 31).strftime('%Y-%m-%d')

        url = f'https://api.aeso.ca/report/v1.1/price/poolPrice?startDate={start_date}&endDate={end_date}'

        headers = {
            'accept': 'application/json',
            'X-API-Key': aeso_key
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()['return']['Pool Price Report']
            # Save the parsed data to a CSV file
            save_to_csv(data, year)
        else:
            print(f'Failed to fetch data for {year}: HTTP {response.status_code}')




