import sqlalchemy
import pymysql

class Config:    
    #Creating a connection to MySql Database
    def sql_connect(self, db_config):
        conn_str = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(db_config['username'],
                                                                db_config['password'],
                                                                   db_config['host'],
                                                                db_config['port'],
                                                                db_config['database'])
        connection = sqlalchemy.create_engine(conn_str)
        return connection

    #Database Credentials
    db_config = { 'username':'root',
                 'password':'rootroot',
                 'host':'localhost',
                 'port':'3306',
                 'database':'superuser'}

    #Country Table Mapping
    countries_lookup = {'USA':'cus_details_usa',
                        'IND': 'cus_details_india',
                        'PHIL': 'cus_details_phil',
                        'NYC': 'cus_details_nyc',
                        'AU': 'cus_details_au'}
    #Stage Table Name
    stage_tbl = 'customer_details_stage'