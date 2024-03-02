"""
    File name: historic_weather_scrape.py
    Author: Hayden Fiege
    Edited by: Brandon Muise
    Date created: 03/01/2021
    Python Version: 3.8.8
    Source: https://github.com/Haydenfiege/utility-forecaster/blob/main/data_collection/historic_weather_scrape.py
"""
import requests


# loop through the selected data range and download the historic hourly weather data for the selected city
def historic_weather_scrape(city, year_start, month_start, year_end, month_end):

    # dictionary for weather stations IDs
    weather_stations = {
        "YYC": "50430",
        "YEG": "27214",
        "YMM": "49490"
    }

    for year in range(year_start, year_end+1):
        for month in range(month_start, month_end+1):

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


if __name__ == '__main__':
    historic_weather_scrape('YYC', 2000, 0, 2023, 12)
