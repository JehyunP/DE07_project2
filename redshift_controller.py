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
            'create_table_merged_marine' : self.create_table_merged_marine,
            'create_table_elt_sfc' : self.create_table_elt_sfc,
            'create_table_elt_marine' : self.create_table_elt_marine,
            'create_table_sfc_temp_change' : self.create_table_sfc_temp_change
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
        query = '''
            DROP TABLE IF EXISTS raw_data.stn_sfc CASCADE;
            CREATE TABLE raw_data.stn_sfc (
                stn_id        INT            PRIMARY KEY,    
                lon_degree    FLOAT,                        
                lat_degree    FLOAT,                       
                stn_sp        VARCHAR(20),                
                ht_m          FLOAT,                   
                ht_pa_m       FLOAT,                  
                ht_ta_m       FLOAT,                    
                ht_wd_m       FLOAT,                    
                ht_rn_m       FLOAT,                     
                stn_ad        VARCHAR(50),            
                stn_ko        VARCHAR(50),                 
                stn_en        VARCHAR(50),                  
                fct_id        VARCHAR(20),                  
                law_id        VARCHAR(20)                   
            )
            DISTSTYLE KEY
            DISTKEY (stn_id)
            SORTKEY (stn_id);
        '''
        try:
            self.cursor.execute(query)

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
                DROP TABLE IF EXISTS raw_data.merged_sfc;
                CREATE TABLE raw_data.merged_sfc (
                    tm DATE,
                    stn INT,

                    ws_avg FLOAT,
                    wr_day FLOAT,
                    wd_max FLOAT,
                    ws_max FLOAT,
                    ws_max_tm VARCHAR(20),
                    wd_ins FLOAT,
                    ws_ins FLOAT,
                    ws_ins_tm VARCHAR(20),
                    ta_avg FLOAT,
                    ta_max FLOAT,
                    ta_max_tm VARCHAR(20),
                    ta_min FLOAT,
                    ta_min_tm VARCHAR(20),
                    td_avg FLOAT,
                    ts_avg FLOAT,
                    tg_min FLOAT,
                    hm_avg FLOAT,
                    hm_min FLOAT,
                    hm_min_tm VARCHAR(20),
                    pv_avg FLOAT,
                    ev_s FLOAT,
                    ev_l FLOAT,
                    fg_dur FLOAT,
                    pa_avg FLOAT,
                    ps_avg FLOAT,
                    ps_max FLOAT,
                    ps_max_tm VARCHAR(20),
                    ps_min FLOAT,
                    ps_min_tm VARCHAR(20),
                    ca_tot FLOAT,
                    ss_day FLOAT,
                    ss_dur FLOAT,
                    ss_cmb FLOAT,
                    si_day FLOAT,
                    si_60m_max FLOAT,
                    si_60m_max_tm VARCHAR(20),
                    rn_day FLOAT,
                    rn_d99 FLOAT,
                    rn_dur FLOAT,
                    rn_60m_max FLOAT,
                    rn_60m_max_tm VARCHAR(20),
                    rn_10m_max FLOAT,
                    rn_10m_max_tm VARCHAR(20),
                    rn_pow_max FLOAT,
                    rn_pow_max_tm VARCHAR(20),
                    sd_new FLOAT,
                    sd_new_tm VARCHAR(20),
                    sd_max FLOAT,
                    sd_max_tm VARCHAR(20),
                    te_05 FLOAT,
                    te_10 FLOAT,
                    te_15 FLOAT,
                    te_30 FLOAT,
                    te_50 FLOAT,

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


    def create_table_elt_sfc(self):
        '''
            Extract data from raw_data to create visualization 
            SFC data sets will be used to transfer into analytics
        '''
        try:
            create_sql = '''
                drop table if exists analytics.sfc;
                CREATE TABLE analytics.sfc as
                    select
                        a.tm as "관측시각",
                        a.stn as "Station",
                        b.stn_ko as "지역",
                        b.lon_degree as "경도",
                        b.lat_degree as "위도",
                        a.ta_avg as "평균기온",
                        a.ta_max as "최고기온",
                        a.ta_min as "최저기온",
                        a.hm_avg as "평균습도",
                        a.rn_d99 as "강수량"
                    from raw_data.merged_sfc a
                    right join raw_data.stn_sfc as b on a.stn = b.stn_id
                    order by a.tm;
                '''
            self.cursor.execute(create_sql)

            self.conn.commit()
            print('ELT Done : Created sfc table!')
            self.save_log('THE analytics.sfc table created')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def create_table_elt_marine(self):
        '''
            Extract data from raw_data to create visualization 
            marine data sets will be used to transfer into analytics
        '''
        try:
            create_sql = '''
                drop table if exists analytics.marine;

                CREATE TABLE analytics.marine as
                    select
                        tm_kst as "관측시각",
                        stn_id as "ID",
                        stn_ko as "지점",
                        lon_deg as "경도",
                        lat_deg as "위도",
                        tw_c as "해수면 온도",
                        ta_c as "기온",
                        hm_percent as "습도"
                    from raw_data.merged_marine
                    order by 1;
                '''
            self.cursor.execute(create_sql)

            self.conn.commit()
            print('ELT Done : Created marine table!')
            self.save_log('THE analytics.marine table created')


        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')


    def create_table_sfc_temp_change(self):
        '''
            ELT -> elt sfc table into calculate each year's temperature differences
        '''
        try:
            create_sql = '''
                drop table if exists analytics.sfc_temp_diff;

                create table analytics.sfc_temp_diff as
                    with year_tmp as (
                        select 
                            date_part('year', "관측시각") as year,
                            avg("최고기온") as high_temp,
                            avg("평균기온") as avg_temp,
                            avg("최저기온") as low_temp
                        from analytics.sfc
                        group by 1
                        order by 1
                    )

                    select
                        year, 
                        round(high_temp, 2) as "최고기온",
                        round(avg_temp, 2) as "평균기온",
                        round(low_temp, 2) as "최저기온",
                        COALESCE(
                            ROUND(high_temp - LAG(high_temp, 1) OVER (ORDER BY year), 2),
                            0
                        ) AS "최고기온 변화량",
                        
                        COALESCE(
                            ROUND(avg_temp - LAG(avg_temp, 1) OVER (ORDER BY year), 2),
                            0
                        ) AS "평균기온 변화량",
                        
                        COALESCE(
                            ROUND(low_temp - LAG(low_temp, 1) OVER (ORDER BY year), 2),
                            0
                        ) AS "최저기온 변화량"
                    from year_tmp
                    order by 1;
            '''
            self.cursor.execute(create_sql)

            self.conn.commit()
            print('ELT Done : Created sfc_temp_diff table!')
            self.save_log('THE analytics.sfc_temp_diff table created')

        except Exception as e:
            print(f'Error Creating Table : {e}')
            self.save_log('Error creating table: {e}')




if __name__ == '__main__':
    redshift_controller()