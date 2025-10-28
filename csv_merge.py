import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np


class csv_merge:
    def merge(self, path, type):
        '''
            Merge all the csv file in the path then create a csv file 

            param
                path : the path of folder having csv files
        '''

        # put all csv file into a list
        csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]

        # put all csv files into lists separated
        df_lists = []

        # Read csv and append at list
        for file in csv_files:
            file_path = os.path.join(path, file)
            df = pd.read_csv(file_path)
            df_lists.append(df)
            print('-------------------- working --------------------')
            

        # merge all csv files in the list
        merged_df = pd.concat(df_lists, ignore_index=True)

        # Cleaning DF if type is marine
        if type.lower() == 'marine':
            print('Processing Marine Dataset..')
            merged_df["TM_KST"] = merged_df["TM_KST"].astype(str).str[:8]

        merged_df.fillna('', inplace=True)
        # create merged data.csv
        merged_df.to_csv(f'data/merged_{type}.csv', index=False, encoding='utf-8')

        # Debug : to see merged csv
        print(f'Merged datasets completed!\n{merged_df.head(5)}')

        


    def __init__(self):
        # Read Path from .env
        load_dotenv()
        files_path_weather = os.getenv('FILES_PATH_weather')
        files_path_marine = os.getenv('FILES_PATH_marine')

        # run function -> merge all csv
        # self.merge(files_path_weather, 'SFC')
        #self.merge(files_path_marine, 'marine')



if __name__ == '__main__':
    csv_merge()