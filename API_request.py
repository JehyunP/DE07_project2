import pandas as pd
import requests
from dotenv import load_dotenv
import os
from collections import defaultdict


class API_Request:


    def request_api_weather (self, tm:str = None , stn:str = None, disp:str = '1', help:str = '1', authKey:str = ''):
        '''
            Request weather data to apihub.kma.go.kr
            The API will return specific weather data for a day
            The data is measured by Ground weather observation

            Param:
                tm : The specific time (YearMonthDay) in KST
                    if none -> Current Time returned
                    else -> 20251010 : the data is measured of 2025-10-10 
                stn : The station of measuring weather condition
                    if None or 0 -> For all station will be returned
                    else -> the station will be separated by ':'
                disp : the format of api
                    1 : In CSV
                    0 : In TSV
                help : Add specification/Info session
                    1 : Include explanation of field
                    0 : Exclude explanation of field
                authkey : The secret key for API request
        '''

        # URL for API
        url = 'https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php?'
        tm_ = (f"tm={tm}&") if tm else ""
        stn_ = (f"stn={stn}&") if stn else ""
        disp_ = f"disp={disp}&"
        help_ = f"help={help}&"
        authKey_ = f"authKey={authKey}"
        ready_url = url + tm_ + stn_ + disp_ + help_ + authKey_

        # Request API from URL
        try:
            response = requests.get(ready_url)
            response.raise_for_status()

            lines = response.text.splitlines()

            # split documentation and main data
            info_lines = [line.lstrip('#') for line in lines if line.startswith('#')]
            data_lines = lines[len(info_lines)-1 : -1]
            info_lines = info_lines[4:-6]

            # Extract header from documentationo
            header_list = []
            for info in info_lines:
                parse = info.split()[1].split()[-1]
                header_list.append(parse)
            

            # each header will take one's list and it will convert into data frame
            data = defaultdict(list)

            # Classification of header and its main info
            for main_data in data_lines:
                for head, dt in zip(header_list, main_data.split(',')):
                    data[head].append(dt)

            # Convert dictionary into data frame
            df = pd.DataFrame(data)

            # Save dataframe as csv
            df.to_csv(f'data/weather_condition/{tm if tm else 'test'}.csv', index=False, encoding='utf-8-sig')

        # Exception catcher
        except Exception as e:
            print(f'Error detected : {e}')



    def request_api_location(self, inf='SFC', tm=None, stn=None, help='0', authKey=''):
        '''
            Request station's location info from apihub.kma.go.kr
            The API will return location of each Station

            Param:
                inf : The information about Stations
                    SFC : Surface (ground Observation)
                    AWS : Automated Weather Station
                    NKO : North Korea
                    UV  : Ultraviolet
                tm : The specific time (YearMonthDay) in KST
                    if none -> Current Time returned
                    else -> 20251010 : the data is measured of 2025-10-10 
                stn : The station of measuring weather condition
                    if None or 0 -> For all station will be returned
                    else -> the station will be separated by ':'
                help : Add specification/Info session
                    1 : Include explanation of field
                    0 : Exclude explanation of field
                authkey : The secret key for API request
        '''
        # URL for API
        url = 'https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?'
        inf_ = f'inf={inf}&'
        tm_ = (f"tm={tm}&") if tm else ""
        stn_ = (f"stn={stn}&") if stn else ""
        help_ = f"help={help}&"
        authKey_ = f"authKey={authKey}"
        ready_url = url + inf_ + tm_ + stn_ + help_ + authKey_

        # Request API via URL
        try:
            response = requests.get(ready_url)
            response.raise_for_status()
            lines = response.text.splitlines()

            # lines of documentation will be separated into header else main
            header = []
            main = []
            for line in lines:
                if '#' in line:
                    header.append(line.lstrip('# '))
                else:
                    main.append(line.strip())
            
            # extract header info - variable names and its units
            header_list = []
            for head, sub in zip(header[-3].split(), header[-2].split()):
                if '-' not in sub:
                    header_list.append(f'{head}_{sub}')
                else:
                    header_list.append(head)
            
            # each header will take one's list and it will convert into data frame
            data = defaultdict(list)

            # Classification of header and its main info
            for main_data in main:
                for head, dt in zip(header_list, main_data.split()):
                    data[head].append(dt)
            
            # Drop useless column
            del data['BASIN']

            # Convert data into dataframe
            df = pd.DataFrame(data)

            # Save dataframe as csv
            df.to_csv('data/stn_info.csv', index=False, encoding='utf-8-sig')

        # Error Catcher
        except Exception as e:
            print(f'Error detected : {e}')

        
        



    def __init__(self):
        # Get API_key from .env
        load_dotenv()
        self.api_key = os.getenv('API_KEY')

        # Run to get stn_location csv
        #self.request_api_location(authKey=self.api_key)

        # Run to get weather api
        self.request_api_weather(authKey=self.api_key)





if __name__ == "__main__":
    api_request = API_Request()