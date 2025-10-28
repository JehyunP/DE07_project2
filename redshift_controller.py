import psycopg2
import os
from datetime import datetime

from dotenv import load_dotenv

class redshift_controller:


    def __init__(self):

        load_dotenv()

        # Log path
        self.log_dir = os.getenv('LOG_HISTORY')
        today_str = datetime.now().strftime('%Y%m%d')
        self.log_file = os.path.join(self.log_dir, f"{today_str}.txt")

        # Mapping methods for easy access
        self.command_map = {
            'create_table_stn_sfc' : self.create_table_stn_sfc,
            'create_table_merged_sfc' : self.create_table_merged_sfc,
            'create_table_merged_marine' : self.create_table_merged_marine
        }

        # If Today's log not exist, create a new txt else just use established file
        if not os.path.exists(self.log_file):
            with open (self.log_file, 'w', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Log file created.\n")
            print(f"New Log data file created: {self.log_file}")
        else:
            print('Log data file already created')

        # Connecting to redshift 
        self.conn = psycopg2.connect(
            host=os.getenv('REDSHIFT_HOST'),
            dbname=os.getenv('REDSHIFT_DB'),
            user=os.getenv('REDSHIFT_USER'),
            password=os.getenv('REDSHIFT_PW'),
            port=os.getenv('REDSHIFT_PORT')
        )

        # Debug : To see connected to redshift -> show current db and access user
        self.cursor = self.conn.cursor()
        print('Connected to Redshift')
        self.cursor.execute("SELECT current_database(), current_user;")
        self.current_db, self.current_user = self.cursor.fetchone()

        self.save_log(f'{self.current_user} just connected to Redshift sever at db[{self.current_db}]')
        self.run_querry()


    def save_log(self, history):
        '''
            Stored the time and history in the log file
            param : 
                history : all the execution from redshift
        '''

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f'[{timestamp}] {history}\n'
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)


    def run_querry(self):
        '''
            Running redshift for executing querries
            Stop run redshift by input quit()
        '''

        while True:
            command = input('SQL Ready (Type quit() to stop access) : ')
            if command.strip().lower() == 'quit()': 
                print('Redshift access terminated')
                self.save_log(f'Session closed by {self.current_user}')
                break

            if command in self.command_map:
                print(f'Calling Function : {command}')
                self.save_log(f'Function {command} has called')
                try:
                    self.command_map[command]() # Run called function
                except Exception as e:
                    self.conn.rollback()
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
                    if self.cursor.description is not None:
                        rows = self.cursor.fetchall()
                        
                        for row in rows:
                            print(row)
                        print(f"{len(rows)} rows returned ({elapsed:.3f}s)")
                    else:
                        print("No result set returned.")
                    self.save_log(f"SUCCESS (SELECT) [{elapsed:.3f}s]: {command}")

                else:
                    # 3) DDL/DML Commit
                    self.conn.commit()
                    print(f"Query executed successfully (committed in {elapsed:.3f}s)")
                    self.save_log(f"SUCCESS (DDL/DML) [{elapsed:.3f}s]: {command}")


            except Exception as e:
                print(f'Error Raised : {e}')
                self.save_log(f'Error Detected : {e} ({command})')

        self.cursor.close()
        self.conn.close()
        print('Connection closed')


    def create_table_stn_sfc(self):
        '''
            Create the table raw_data.stn_info if not exists,
            copy from S3 called stn_SFC_info.csv
        '''
        try:
            self.cursor.execute('''
                drop table if exists raw_data.stn_sfc;
                create table raw_data.stn_sfc (
                    stn_id INT primary key,
                    lon_degree FLOAT,
                    lat_degree FLOAT,
                    stn_ko VARCHAR(50)
                );
            ''')

            self.conn.commit()
            print('The stn sfc data created!')
            self.save_log('THE raw_data.stn_sfc table created')

            # Copy from S3
            copy_sql = f'''
                COPY raw_data.stn_sfc
                from '{os.getenv('STN_SFC')}'
                IAM_ROLE '{os.getenv('REDSHIFT_ROLE_ARN')}'
                DELIMITER ','
                DATEFORMAT 'auto'
                TIMEFORMAT 'auto'
                IGNOREHEADER 1
                REMOVEQUOTES;
            '''

            self.cursor.execute(copy_sql)
            self.conn.commit()

            print('Data copied from S3 -> raw_data.stn_sfc')
            self.save_log('Copied data from S3 to raw_data.stn_sfc')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def create_table_merged_sfc(self):
        '''
            Create the table raw_data.merged_SFC if not exists,
            copy from S3 called merged_SFC.csv
            this table is mapping with stn_sfc by stn
        '''
        try:
            create_sql = '''
                drop table if exists raw_data.merged_sfc;
                create table raw_data.merged_sfc(
                    tm Date, 
                    stn Int,
                    pa float,
                    ps FLOAT,
                    pr FLOAT,
                    ta FLOAT,
                    td FLOAT,
                    hm FLOAT,
                    pv FLOAT,
                    rn FLOAT,
                    rn_int FLOAT,
                    sd_hr3 FLOAT,
                    sd_day FLOAT,
                    sd_tot FLOAT,
                    ts FLOAT,
                    PRIMARY KEY (tm, stn),
                    FOREIGN KEY (stn) REFERENCES raw_data.stn_sfc(stn_id)
                )
                DISTSTYLE KEY
                DISTKEY (stn)
                SORTKEY (tm);
            '''
            self.cursor.execute(create_sql)

            self.conn.commit()
            print('The merged_sfc data created!')
            self.save_log('THE raw_data.merged_sfc table created')

            # Copy from S3
            copy_sql = f'''
                COPY raw_data.merged_sfc
                from '{os.getenv('MERGED_SFC')}'
                IAM_ROLE '{os.getenv('REDSHIFT_ROLE_ARN')}'
                DELIMITER ','
                DATEFORMAT 'auto'
                TIMEFORMAT 'auto'
                IGNOREHEADER 1
                REMOVEQUOTES;
            '''

            self.cursor.execute(copy_sql)
            self.conn.commit()

            print('Data copied from S3 -> raw_data.merged_sfc')
            self.save_log('Copied data from S3 to raw_data.merged_sfc')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def create_table_merged_marine(self):
        '''
            Create the table raw_data.merged_marine if not exists,
            copy from S3 called merged_marine.csv
        '''
        try:
            create_sql = '''
                drop table if exists raw_data.merged_marine;
                CREATE TABLE raw_data.merged_marine (
                    tm_kst     TIMESTAMP,     
                    stn_id     INT,          
                    stn_ko     VARCHAR(50),   
                    lon_deg    FLOAT,          
                    lat_deg    FLOAT,       
                    wh_m       FLOAT,    
                    wd_deg     FLOAT,    
                    ws_m_s     FLOAT,
                    ws_gst     FLOAT,       
                    tw_c       FLOAT,      
                    ta_c       FLOAT,        
                    pa_hpa     FLOAT,    
                    hm_percent FLOAT,     
                    PRIMARY KEY (tm_kst, stn_id)
                )
                DISTSTYLE KEY
                DISTKEY (stn_id)
                SORTKEY (tm_kst);
            '''
            self.cursor.execute(create_sql)

            self.conn.commit()
            print('The merged_marine data created!')
            self.save_log('THE raw_data.merged_marine table created')

            # Copy from S3
            copy_sql = f'''
                COPY raw_data.merged_marine
                from '{os.getenv('MERGED_MARINE')}'
                IAM_ROLE '{os.getenv('REDSHIFT_ROLE_ARN')}'
                DELIMITER ','
                DATEFORMAT 'auto'
                TIMEFORMAT 'auto'
                IGNOREHEADER 1
                REMOVEQUOTES;
            '''

            self.cursor.execute(copy_sql)
            self.conn.commit()

            print('Data copied from S3 -> raw_data.merged_marine')
            self.save_log('Copied data from S3 to raw_data.merged_marine')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')



if __name__ == '__main__':
    redshift_controller()