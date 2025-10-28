import os, boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError


class load_into_s3:

    def __init__(self):
        print(' ------ Preparing to load the data into S3. ------')

        # Get significant data from .env
        load_dotenv()
        self.key = os.getenv('AWS_KEY')
        self.secret_key = os.getenv('AWS_SECRETE_KEY')
        self.region = os.getenv('AWS_REGION')
        self.bucket = os.getenv('AWS_BUCKET')

        # The file paths of data and its target directory in S3
        dirs = {
            os.getenv('MERGED_FILE_SFC_PATH') : 'raw/merged_SFC.csv',
            os.getenv('MERGED_FILE_MARINE_PATH') : 'raw/merged_marine.csv',
            os.getenv('STN_SFC_FILE_PATH') : 'raw/stn_SFC_info.csv',
            os.getenv('STN_BUOY_FILE_PATH') : 'raw/stn_BUOY_info.csv',
            os.getenv('META_MARINE_PATH') : 'raw/marine_meta.txt',
            os.getenv('META_WEATHER_PATH') : 'raw/weather_meta.txt'
        }

        # Connect to AWS S3 with key
        s3 = boto3.client(
            's3',
            aws_access_key_id = self.key,
            aws_secret_access_key = self.secret_key,
            region_name = self.region
        )

        # Run load_data into S3 
        for dir, s3_loc in dirs.items():
            self.load(s3, dir, s3_loc)
        

    def load(self, client, dir, s3_loc):
        '''
            Uploading(Load) process into AWS S3 bucket -> last step for ETL
            Condition Check :   
                If uploading file already exists in S3 directory -> does not upload
                If uploading file already exists but file size is different between
                    local and S3 -> Deleting file in S3 and upload new data 
                    (Showing the data has updated)
                If uploading file is not existed in S3 directory -> Upload
            
            param :
                client : the connected AWS server with boto3
                dir : local directory of uploading data
                s3_loc : the target directory in S3
        '''
        
        # Get size of local data
        local_size = os.path.getsize(dir)
        
        try:
            # Request data from S3 to see the uploading data already in S3 bucket
            
            # Get data in S3 bucket size
            response = client.head_object(Bucket=self.bucket, Key=s3_loc)
            s3_size = response['ContentLength']

            # First condition check
            if local_size == s3_size:
                print(f'{os.path.basename(dir)} : Already exists in S3 -> Skip UPLOAD')
                return
            else: # Second condition check
                print(f'{os.path.basename(dir)} : File size is different -> Delete and UPLOAD New data')
                client.delete_object(Bucket=self.bucket, Key=s3_loc)

        except ClientError as e:
            # Last condition check
            if e.response['Error']['Code'] == '404':
                print(f'{os.path.basename(dir)} : does not exist in S3 -> UPLOAD')
            else:
                raise

        # Uploading data into S3 bucket
        client.upload_file(dir, self.bucket, s3_loc)
        print(f'The file({os.path.basename(dir)}) Uploading in progress at {s3_loc}')



if __name__ == '__main__':
    load_into_s3()