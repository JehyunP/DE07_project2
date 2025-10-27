import pandas as pd
import requests, os, time
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime, timedelta
import csv
from io import StringIO

class API_Request:


    def request_api_weather (self, tm:str = None , stn:str = None, disp:str = '1', help:str = '1'):
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
        '''

        # URL for API
        url = 'https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php?'
        tm_ = (f"tm={tm}&") if tm else ""
        stn_ = (f"stn={stn}&") if stn else ""
        disp_ = f"disp={disp}&"
        help_ = f"help={help}&"
        authKey_ = f"authKey={self.api_key}"
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
            df.to_csv(f'data/weather_condition/{tm}.csv', index=False, encoding='utf-8-sig')

            return True

        # Exception catcher
        except Exception as e:
            print(f'Error detected : {e}')
            return False


    def request_api_location(self, inf='SFC', tm=None, stn=None, help='0'):
        '''
            Request station's location info from apihub.kma.go.kr
            The API will return location of each Station

            Param:
                inf : The information about Stations
                    SFC : Surface (ground Observation)
                    AWS : Automated Weather Station
                    NKO : North Korea
                    UV  : Ultraviolet
                    BUOY : Marine
                tm : The specific time (YearMonthDay) in KST
                    if none -> Current Time returned
                    else -> 20251010 : the data is measured of 2025-10-10 
                stn : The station of measuring weather condition
                    if None or 0 -> For all station will be returned
                    else -> the station will be separated by ':'
                help : Add specification/Info session
                    1 : Include explanation of field
                    0 : Exclude explanation of field
        '''
        # URL for API
        url = 'https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?'
        inf_ = f'inf={inf}&'
        tm_ = (f"tm={tm}&") if tm else ""
        stn_ = (f"stn={stn}&") if stn else ""
        help_ = f"help={help}&"
        authKey_ = f"authKey={self.api_key}"
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
            # For SFC
            if inf == 'SFC':
                del data['BASIN']

            # Convert data into dataframe
            df = pd.DataFrame(data)

            # Save dataframe as csv
            df.to_csv(f'data/stn_{inf}_info.csv', index=False, encoding='utf-8-sig')
            print(f'Working Done - {inf}')
            return True

        # Error Catcher
        except Exception as e:
            print(f'Error detected : {e}')
            return False


    def request_api_loop(self, initial_date, end_date):
        '''
            Run the request api weather class for duration(initial date ~ end date)

            param:
                initial_date : first day for request
                end_date : last day for request
        '''

        # Set the format for Date
        fmt = "%Y-%m-%d" if "-" in initial_date else "%Y%m%d"

        # Cast initial and end date into datetime
        initial_date = datetime.strptime(initial_date, fmt)
        end_date = datetime.strptime(end_date, fmt)

        # Will be used for time calculation
        start = time.time()

        # Loop for set dates
        while initial_date <= end_date:
            
            print(f'[Fetching : {initial_date}] initiated . . .')

            # set the datetime into yyyymmdd format for api request
            yyyymmdd = initial_date.strftime("%Y%m%d")

            # run request
            result = self.request_api_weather(tm=yyyymmdd)

            # Debug -> to see how requests going on
            if result:
                print(f'Succeed to get respond from API request at {yyyymmdd}')
            else:
                print(f'Request Fail at {yyyymmdd}')
                break

            # increase date by a day
            initial_date += timedelta(days=1)

        # end time for time cacluation
        end = time.time()

        # Calculate time spent
        elapse = end - start
        print(f'Total Run time : {elapse: .2f} sec')


    def request_api_marine(self, tm=None, stn=None, help='0'):
        '''
            Request the overall marine observation data
            The data is observed wihtin every minute 

            tm : The specific time (YearMonthDayHourMin) in KST
                if none -> Current Time returned
                else -> 202510101500 : the data is measured of 2025-10-10 15:00
            stn : The station of measuring weather condition
                if None or 0 -> For all station will be returned
                else -> the station will be separated by ':'
            help : Add specification/Info session
                1 : Include explanation of field -> Add meta.txt if not exist
                0 : Exclude explanation of field
        '''

        # URL of Endpoint
        url = 'https://apihub.kma.go.kr/api/typ01/url/sea_obs.php?'
        tm_ = (f"tm={tm}&") if tm else ""
        stn_ = (f"stn={stn}&") if stn else ""
        help_ = f"help={help}&"
        authKey_ = f"authKey={self.api_key}"
        ready_url = url + tm_ + stn_ + help_ + authKey_

        # Request API from URL
        try:
            response = requests.get(ready_url)
            response.raise_for_status()

            lines = response.text.splitlines()

            # Split data by documentation
            meta, main_data = [], []
            for line in lines:
                meta.append(line.lstrip('# ')) if line.startswith('#') else main_data.append(line)
                    
            # if help = 1 : make meta info for getting data
            if help:
                with open(os.getenv('FILES_PATH_marine') + '\\marine_meta.txt', 'w', encoding='utf-8') as f:
                    f.write('\n'.join(meta[5:-3]))
            
            header_list = []

            # Merge 2 lines into column names
            for head, sub in zip(meta[-3].split(), meta[-2].split()):
                header_list.append(f'{head}_{sub}')

            data = defaultdict(list)

            # remove all ',' and split by any spaces
            for main in main_data:
                parse = main.replace(',','') 

                # insert main value into following column
                for key, val in zip(header_list, parse.split()[1:-1]):
                    data[key].append(val)

            # Convert dictionary into data frame
            df = pd.DataFrame(data)

            # Save dataframe as csv
            df.to_csv(f'data/marine_condition/{tm}.csv', index=False, encoding='utf-8-sig')

            return True

        # Exception catcher
        except Exception as e:
            print(f'Error detected : {e}')
            return False
        

    def request_api_loop_marine(self, initial_date, end_date):
        '''
            Run the request api marine class for duration(initial date ~ end date)
            The date format is yyyymmddhhmm

            param:
                initial_date : first day for request
                end_date : last day for request
        '''

        # Set the format for Date
        fmt = "%Y-%m-%d-%H-%M" if "-" in initial_date else "%Y%m%d%H%M"


        # Cast initial and end date into datetime
        initial_date = datetime.strptime(initial_date, fmt)
        end_date = datetime.strptime(end_date, fmt)

        # Will be used for time calculation
        start = time.time()

        # Loop for set dates
        while initial_date <= end_date:
            
            print(f'[Fetching : {initial_date}] initiated . . .')

            # set the datetime into yyyymmdd format for api request
            yyyymmddhhmm = initial_date.strftime("%Y%m%d%H%M")

            # run request
            result = self.request_api_marine(tm=yyyymmddhhmm)

            # Debug -> to see how requests going on
            if result:
                print(f'Succeed to get respond from API request at {yyyymmddhhmm}')
            else:
                print(f'Request Fail at {yyyymmddhhmm}')
                break

            # increase date by a day
            initial_date += timedelta(days=1)

        # end time for time cacluation
        end = time.time()

        # Calculate time spent
        elapse = end - start
        print(f'Total Run time : {elapse: .2f} sec')    
    


    def __init__(self):
        # Get API_key from .env
        load_dotenv()
        self.api_key = os.getenv('API_KEY')
        

        # # Run to get continent data csv
        # # To see stn_info for weather already exist
        # weather_stn = True if os.path.exists(os.path.join(
        #     os.getenv('FILES_PATH_STN'), 'stn_SFC_info.csv')) else False
        # if not weather_stn:
        #     self.request_api_location(inf='SFC')
        # else:
        #     print('File already in path')
        # # Run to get weather api
        # self.request_api_loop('2015-01-01','2025-10-25')

        self.request_api_loop_marine('2015-01-01-14-00','2025-10-25-14-00')


if __name__ == "__main__":
    api_request = API_Request()