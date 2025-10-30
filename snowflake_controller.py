import snowflake.connector
import os
from datetime import datetime
from dotenv import load_dotenv

class snowflake_controller:


    def __init__(self):

        load_dotenv()

        # Log path
        self.log_dir = os.getenv('LOG_HISTORY')
        os.makedirs(self.log_dir, exist_ok=True)
        today_str = datetime.now().strftime('%Y%m%d')
        self.log_file = os.path.join(self.log_dir, f"{today_str}.txt")

        # Mapping methods for easy access
        self.command_map = {
            'station_surface_kor' : self.station_surface_kor,
            'surface_kor' : self.surface_kor,
            'marine_kor' : self.marine_kor,
            'surface_kor_daily_analytics' : self.surface_kor_daily_analytics,
            'marine_kor_daily_analytics' : self.marine_kor_daily_analytics,
            'surface_kor_annualy_temperature' : self.surface_kor_annualy_temperature
        }

        # If Today's log not exist, create a new txt else just use established file
        if not os.path.exists(self.log_file):
            with open (self.log_file, 'w', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Log file created.\n")
            print(f"New Log data file created: {self.log_file}")
        else:
            print('Log data file already created')

        # Connecting to redshift 
        self.conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )

        # Debug : To see connected to redshift -> show current db and access user
        self.cursor = self.conn.cursor()

        # Check Stage -> create Stage
        self.ensure_stage_exists()

        print('Connected to Snowflake')

        self.save_log(f'just connected to snowflake sever at db')


        self.run_querry()


    def ensure_stage_exists(self):
        """
            Check if Stage already exists in Snowflake; if not, create one.
        """
        try:
            stage_check = '''
                SHOW STAGES LIKE 'my_s3_stage';
            '''
            self.cursor.execute(stage_check)
            result = self.cursor.fetchall()

            if len(result) == 0:
                print('Stage Not Found -> Creating new stage \'my_s3_stage\'')
                stage_sql = f'''
                    CREATE OR REPLACE STAGE RAW_DATA.MY_S3_STAGE
                    URL='s3://{os.getenv("AWS_BUCKET")}/raw/'
                    CREDENTIALS=(
                        AWS_KEY_ID='{os.getenv("AWS_KEY")}'
                        AWS_SECRET_KEY='{os.getenv("AWS_SECRETE_KEY")}'
                    )
                    FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
                '''
                self.cursor.execute(stage_sql)
                print("Stage 'my_s3_stage' created successfully")
                self.save_log("Stage 'my_s3_stage' created successfully")
            else:
                print("Stage 'my_s3_stage' already exists (skipped creation)")
                self.save_log("Stage 'my_s3_stage' already exists")

        except Exception as e:
            print(f"Error verifying/creating stage: {e}")
            self.save_log(f"Error verifying/creating stage: {e}")


    def save_log(self, history):
        '''
            Stored the time and history in the log file
            param : 
                history : all the execution from snowflake
        '''

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f'[{timestamp}] {history}\n')


    def run_querry(self):
        '''
            Running snowflake for executing querries
            Stop run snowflake by input quit()
        '''

        while True:
            command = input('SQL Ready (Type quit() to stop access) : ')
            if command.strip().lower() == 'quit()': 
                print('snowflake access terminated')
                self.save_log(f'Session closed')
                break

            if command in self.command_map:
                print(f'Calling Function : {command}')
                self.save_log(f'Function {command} has been called')
                try:
                    self.command_map[command]() # Run called function
                except Exception as e:
                    print(f'Error Running {command} : {e}')
                    self.save_log(f'Error in {command} : {e}')
                continue
            
            try:

                # Execute Querry
                start_ts = datetime.now()
                self.cursor.execute(command)
                elapsed = (datetime.now() - start_ts).total_seconds()


                # SELECT query output
                if command.strip().lower().startswith("select"):
                    # Fetch if there is meta data
                    rows = self.cursor.fetchall()
                    if rows:
                        for row in rows:
                            print(row)
                        print(f"{len(rows)} rows returned ({elapsed:.3f}s)")
                    else :
                        print("No result set returned")
                    self.save_log(f"SUCCESS (SELECT) [{elapsed:.3f}s]: {command}")

                else:
                    # 3) DDL/DML Commit
                    print(f"Query executed successfully (committed in {elapsed:.3f}s)")
                    self.save_log(f"SUCCESS (DDL/DML) [{elapsed:.3f}s]: {command}")

            except Exception as e:
                print(f'Error Raised : {e}')
                self.save_log(f'Error Detected : {e} ({command})')

        self.cursor.close()
        self.conn.close()
        print('Connection closed')


    def station_surface_kor(self):
        '''
            Create the table raw_data.station_surface_kor if not exists,
            copy from S3 called stn_SFC_info.csv
        '''
        create_sql = """
            CREATE OR REPLACE TABLE RAW_DATA.station_surface_kor (
                stn_id INT,
                lon_degree FLOAT,
                lat_degree FLOAT,
                stn_sp STRING,
                ht_m FLOAT,
                ht_pa_m FLOAT,
                ht_ta_m FLOAT,
                ht_wd_m FLOAT,
                ht_rn_m FLOAT,
                stn_ad STRING,
                stn_ko STRING,
                stn_en STRING,
                fct_id STRING,
                law_id STRING
            );
        """
        try:
            self.cursor.execute(create_sql)
            print('The station_surface_kor data created!')
            self.save_log('The raw_data.station_surface_kor table created')

            # Copy from S3
            copy_sql = f'''
                COPY INTO RAW_DATA.station_surface_kor
                FROM @my_s3_stage/stn_SFC_info.csv
                FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
                ON_ERROR='CONTINUE';
            '''

            self.cursor.execute(copy_sql)

            print("Data copied → RAW_DATA.station_surface_kor")
            self.save_log("station_surface_kor table created and data copied")


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def surface_kor(self):
        '''
            Create the table raw_data.surface_kor if not exists,
            copy from S3 called merged_SFC.csv
        '''
        try:
            create_sql = """
                CREATE OR REPLACE TABLE RAW_DATA.surface_kor (
                    TM DATE,
                    STN INT,
                    WS_AVG FLOAT,
                    WR_DAY FLOAT,
                    WD_MAX FLOAT,
                    WS_MAX FLOAT,
                    WS_MAX_TM STRING,
                    WD_INS FLOAT,
                    WS_INS FLOAT,
                    WS_INS_TM STRING,
                    TA_AVG FLOAT,
                    TA_MAX FLOAT,
                    TA_MAX_TM STRING,
                    TA_MIN FLOAT,
                    TA_MIN_TM STRING,
                    TD_AVG FLOAT,
                    TS_AVG FLOAT,
                    TG_MIN FLOAT,
                    HM_AVG FLOAT,
                    HM_MIN FLOAT,
                    HM_MIN_TM STRING,
                    PV_AVG FLOAT,
                    EV_S FLOAT,
                    EV_L FLOAT,
                    FG_DUR FLOAT,
                    PA_AVG FLOAT,
                    PS_AVG FLOAT,
                    PS_MAX FLOAT,
                    PS_MAX_TM STRING,
                    PS_MIN FLOAT,
                    PS_MIN_TM STRING,
                    CA_TOT FLOAT,
                    SS_DAY FLOAT,
                    SS_DUR FLOAT,
                    SS_CMB FLOAT,
                    SI_DAY FLOAT,
                    SI_60M_MAX FLOAT,
                    SI_60M_MAX_TM STRING,
                    RN_DAY FLOAT,
                    RN_D99 FLOAT,
                    RN_DUR FLOAT,
                    RN_60M_MAX FLOAT,
                    RN_60M_MAX_TM STRING,
                    RN_10M_MAX FLOAT,
                    RN_10M_MAX_TM STRING,
                    RN_POW_MAX FLOAT,
                    RN_POW_MAX_TM STRING,
                    SD_NEW FLOAT,
                    SD_NEW_TM STRING,
                    SD_MAX FLOAT,
                    SD_MAX_TM STRING,
                    TE_05 FLOAT,
                    TE_10 FLOAT,
                    TE_15 FLOAT,
                    TE_30 FLOAT,
                    TE_50 FLOAT
                );
            """
            self.cursor.execute(create_sql)

            print('The surface_kor data created!')
            self.save_log('THE raw_data.surface_kor table created')

            # Copy from S3
            copy_sql = """
                COPY INTO RAW_DATA.surface_kor
                FROM @my_s3_stage/merged_SFC.csv
                FILE_FORMAT=(TYPE=CSV 
                FIELD_OPTIONALLY_ENCLOSED_BY='"' 
                SKIP_HEADER=1
                DATE_FORMAT = YYYYMMDD)
                ON_ERROR='CONTINUE';
            """

            self.cursor.execute(copy_sql)
            print("Data copied → RAW_DATA.surface_kor")
            self.save_log("surface_kor table created and data copied")


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def marine_kor(self):
        '''
            Create the table raw_data.marine_kor if not exists,
            copy from S3 called merged_marine.csv
        '''
        try:
            create_sql = """
                CREATE OR REPLACE TABLE RAW_DATA.marine_kor (
                    TM_KST DATE,
                    STN_ID INT,
                    STN_KO STRING,
                    LON_DEG FLOAT,
                    LAT_DEG FLOAT,
                    WH_M FLOAT,
                    WD_DEG FLOAT,
                    WS_M_S FLOAT,
                    WS_GST FLOAT,
                    TW_C FLOAT,
                    TA_C FLOAT,
                    PA_HPA FLOAT,
                    HM_PERCENT FLOAT
                );
            """
            self.cursor.execute(create_sql)

            print('The marine_kor data created!')
            self.save_log('THE raw_data.marine_kor table created')

            # Copy from S3
            copy_sql = """
                COPY INTO RAW_DATA.marine_kor
                FROM @my_s3_stage/merged_marine.csv
                FILE_FORMAT=(TYPE=CSV 
                FIELD_OPTIONALLY_ENCLOSED_BY='"' 
                SKIP_HEADER=1
                DATE_FORMAT = YYYYMMDD)
                ON_ERROR='CONTINUE';
            """

            self.cursor.execute(copy_sql)

            print("Data copied → RAW_DATA.marine_kor")
            self.save_log("marine_kor table created and data copied")


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def surface_kor_daily_analytics(self):
        '''
            Extract data from raw_data to create visualization 
            surface data sets will be used to transfer into analytics
        '''
        try:
            create_sql = """
                CREATE OR REPLACE TABLE ANALYTICS.surface_kor_daily_analytics AS
                SELECT
                    a.TM AS "관측시각",
                    a.STN AS "Station",
                    b.STN_KO AS "지역",
                    b.LON_DEGREE AS "경도",
                    b.LAT_DEGREE AS "위도",
                    a.TA_AVG AS "평균기온",
                    a.TA_MAX AS "최고기온",
                    a.TA_MIN AS "최저기온",
                    a.HM_AVG AS "평균습도",
                    a.RN_D99 AS "강수량"
                FROM RAW_DATA.surface_kor a
                LEFT JOIN RAW_DATA.station_surface_kor b ON a.STN = b.STN_ID
                ORDER BY a.TM;
            """
            self.cursor.execute(create_sql)

            print('ELT Done : Created surface_kor_daily_analytics table!')
            self.save_log('THE analytics.surface_kor_daily_analytics table created')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def marine_kor_daily_analytics(self):
        '''
            Extract data from raw_data to create visualization 
            marine_kor data sets will be used to transfer into analytics
        '''
        try:
            create_sql = """
                CREATE OR REPLACE TABLE ANALYTICS.marine_kor_daily_analytics AS
                SELECT
                    TM_KST AS "관측시각",
                    STN_ID AS "ID",
                    STN_KO AS "지점",
                    LON_DEG AS "경도",
                    LAT_DEG AS "위도",
                    TW_C AS "해수면 온도",
                    TA_C AS "기온",
                    HM_PERCENT AS "습도"
                FROM RAW_DATA.marine_kor
                ORDER BY TM_KST;
            """
            self.cursor.execute(create_sql)

            print('ELT Done : Created marine_kor_daily_analytics table!')
            self.save_log('THE analytics.marine_kor_daily_analytics table created')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def surface_kor_annualy_temperature(self):
        '''
            ELT -> elt surface_kor_daily_analytics table into calculate 
            each year's temperature differences
        '''
        try:
            create_sql = """
                CREATE OR REPLACE TABLE ANALYTICS.surface_kor_annualy_temperature AS
                WITH year_tmp AS (
                    SELECT
                        YEAR(TO_DATE("관측시각")) AS year,
                        AVG("최고기온") AS high_temp,
                        AVG("평균기온") AS avg_temp,
                        AVG("최저기온") AS low_temp
                    FROM ANALYTICS.surface_kor_daily_analytics
                    WHERE "최고기온" >= -20
                    AND "평균기온" >= -20
                    AND "최저기온" >= -20
                    GROUP BY 1
                    ORDER BY 1
                )
                SELECT
                    year,
                    ROUND(high_temp, 2) AS "최고기온",
                    ROUND(avg_temp, 2) AS "평균기온",
                    ROUND(low_temp, 2) AS "최저기온",
                    COALESCE(ROUND(high_temp - LAG(high_temp) OVER (ORDER BY year), 2), 0) AS "최고기온 변화량",
                    COALESCE(ROUND(avg_temp - LAG(avg_temp) OVER (ORDER BY year), 2), 0) AS "평균기온 변화량",
                    COALESCE(ROUND(low_temp - LAG(low_temp) OVER (ORDER BY year), 2), 0) AS "최저기온 변화량"
                FROM year_tmp
                ORDER BY year;
            """
            self.cursor.execute(create_sql)

            print('ELT Done : Created surface_kor_annualy_temperature table!')
            self.save_log('THE analytics.surface_kor_annualy_temperature table created')

        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')




if __name__ == '__main__':
    snowflake_controller()