import pandas as pd
import os
from dotenv import load_dotenv


class csv_merge:
    def merge(self, path):
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

        # create merged data.csv
        merged_df.to_csv('data/merged.csv', index=False, encoding='utf-8-sig')

        # Debug : to see merged csv
        print(f'Merged datasets completed!\n{merged_df.head(5)}')


    def __init__(self):
        # Read Path from .env
        load_dotenv()
        files_path = os.getenv('FILES_PATH')

        # run function -> merge all csv
        self.merge(files_path)


if __name__ == '__main__':
    csv_merge()