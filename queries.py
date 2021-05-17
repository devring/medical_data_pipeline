from db_config import Config
class Queries:
    
    def __init__(self):
        self.config = Config()
    
    def query_gen(self, query_type ,country):
        
        if query_type == 'insert':
            #Query used to insert only New Records to the corresponding Country Tables
            insert_query = "INSERT INTO {country_table} SELECT t1.Customer_Name, t1.Customer_Id, t1.Open_Date, t1.Last_Consulted_Date, t1.Vaccination_Id, t1.Dr_Name,t1.State, t1.Country, t1.DOB, t1.Is_Active  FROM {stage_table} t1 LEFT JOIN {country_table} t2 on t1.Customer_Name = t2.Customer_Name WHERE t2.Customer_Name is NULL and t1.Country = '{country_name}'".format(country_table = self.config.countries_lookup[country],stage_table = self.config.stage_tbl, country_name = country )
            return insert_query
