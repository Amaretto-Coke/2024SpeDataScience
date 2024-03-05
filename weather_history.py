"""
    File name: historic_weather_scrape.py
    Author: Hayden Fiege
    Edited by: Brandon Muise
    Date created: 03/01/2021
    Python Version: 3.8.8
    Source: https://github.com/Haydenfiege/utility-forecaster/blob/main/data_collection/historic_weather_scrape.py
"""
import requests
from io import StringIO
import warnings
import pandas as pd
from tqdm import tqdm
from time import sleep


# loop through the selected data range and download the historic hourly weather data for the selected city
def historic_weather_scrape(city, year_start, month_start, year_end, month_end):

    # dictionary for weather stations IDs
    weather_stations = {
        "YYC": "50430",
        "YEG": "27214",
        "YMM": "49490",
    }

    for year in tqdm(range(year_start, year_end+1)):
        for month in tqdm(range(month_start, month_end+1), leave=False):

            # generate the url for the requested csv
            csv_url = (f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID="
                       f"{weather_stations[city]}&Year={year}&Month={month}"
                       f"&Day=1&time=LST&timeframe=1&submit=Download+Data")

            # read in url contents with requests
            req = requests.get(csv_url)
            url_content = req.content

            # open a csv with programmatic name
            file_name = rf'WeatherDataRaw\gc_{city}_hourly_weather_{year}_{month}.csv'

            with open(file_name, 'wb') as csv_file:
                csv_file.write(url_content)


def id_weather_stations(nums=None):
    """
    Retrieves and compiles information about Canadian weather stations into a DataFrame.

    This function attempts to download CSV data for a range of weather station IDs from the
    climate.weather.gc.ca website, extracting key details such as station name, latitude, and
    longitude. It handles both successful and unsuccessful data retrieval attempts by marking
    the status of each fetch operation. Unsuccessful attempts due to either bad responses or
    missing data are noted, and all attempts, regardless of outcome, are logged in the DataFrame.

    Parameters:
    - nums (list of int, optional): A custom list of station IDs to query. If not provided,
      the function defaults to querying station IDs in the range 0 to 99,999.

    Returns:
    - pandas.DataFrame: A DataFrame containing the station ID, name, latitude (y), longitude (x),
      and the status of the data retrieval attempt ('Success', 'Empty' for missing data, or
      'Bad Response' for unsuccessful HTTP requests) for each station ID queried.
    """
    if nums is None:
        nums = range(0, 100000)

    columns = ['Station Id', 'Station Name', 'Longitude (x)', 'Latitude (y)', 'Status']
    weather_stations_df = pd.DataFrame(columns=columns)

    for i, num in enumerate(tqdm(nums)):
        num_str = f"{num:05d}"
        year = 2024
        month = 3
        csv_url = (f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID="
                   f"{num_str}&Year={year}&Month={month}&Day=1&time=LST&timeframe=1&submit=Download+Data")

        req = requests.get(csv_url)

        if req.ok and req.text:
            try:
                data = pd.read_csv(StringIO(req.text))

                if 'Longitude (x)' in data.columns and 'Latitude (y)' in data.columns and 'Station Name' in data.columns:
                    station_name = data['Station Name'].iloc[0]
                    longitude = data['Longitude (x)'].iloc[0]
                    latitude = data['Latitude (y)'].iloc[0]
                    status = 'Success'
                else:
                    station_name = longitude = latitude = None
                    status = 'Empty'
            except pd.errors.EmptyDataError:
                station_name = longitude = latitude = None
                status = 'Empty'
                print(f"No data for station ID {num_str}")
        else:
            station_name = longitude = latitude = None
            status = 'Bad Response'

        weather_stations_df.loc[i] = [num_str, station_name, longitude, latitude, status]

    return weather_stations_df


def add_province_via_google_maps(df, api_key):
    """
    Adds a 'Province' column to the DataFrame based on latitude and longitude using the Google Maps API.

    Parameters:
    - df (pd.DataFrame): DataFrame containing weather station data with 'Latitude (y)' and 'Longitude (x)'.
    - api_key (str): Your Google Maps API key.

    Returns:
    - pd.DataFrame: The original DataFrame augmented with a 'Province' column.
    """

    def get_province(lat, lon):
        base_url = "https://maps.googleapis.com/maps/api/geocode/json?"
        complete_url = f"{base_url}latlng={lat},{lon}&key={api_key}"
        response = requests.get(complete_url)
        result = response.json()

        if result["status"] == "OK":
            # Extract the province from the API response
            for component in result["results"][0]["address_components"]:
                if "administrative_area_level_1" in component["types"]:
                    return component["short_name"]  # Province abbreviation
            return None  # In case the province isn't found in the components
        else:
            return None  # In case of a bad response

    # Apply the function to each row in the DataFrame and create the 'Province' column
    df['Province'] = df.apply(lambda row: get_province(row['Latitude (y)'], row['Longitude (x)']), axis=1)

    return df


if __name__ == '__main__':
    '''
    # Ignore FutureWarning
    warnings.filterwarnings("ignore", category=FutureWarning)
    step = 500
    for i in range(22001, 100_001, step):
        print(f"\nGrabbing Stations {i} to {i+step-1}.")
        wsdf = id_weather_stations(list(range(i, i+step)))
        wsdf.to_csv(rf'WeatherStationsListing\{wsdf["Station Id"].iloc[-1]}.csv')
        sleep(2)
    '''

    # historic_weather_scrape("YYC", 2000, 0, 2023, 12)
    historic_weather_scrape("YEG", 2000, 0, 2023, 12)
    historic_weather_scrape("YMM", 2000, 0, 2023, 12)

    print("Done")
