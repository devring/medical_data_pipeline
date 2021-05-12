from db_config import Config
class Queries:
    
    def __init__(self):
        self.config = Config()
    
    def query_gen(self, query_type ,country):
        
        if query_type == 'insert':
            #Query used to insert only New Records to the corresponding Country Tables
            insert_query = "INSERT INTO {country_table} SELECT t1.Customer_Name, t1.Customer_Id, t1.Open_Date, t1.Last_Consulted_Date, t1.Vaccination_Id, t1.Dr_Name,t1.State, t1.Country, t1.DOB, t1.Is_Active  FROM {stage_table} t1 LEFT JOIN {country_table} t2 on t1.Customer_Name = t2.Customer_Name WHERE t2.Customer_Name is NULL and t1.Country = '{country_name}'".format(country_table = self.config.countries_lookup[country],stage_table = self.config.stage_tbl, country_name = country )
            return insert_query
        
        if query_type == 'update': 
            #Updating existing records in Country Table (if any)
            update_query = "INSERT INTO {country_table} SELECT t1.Customer_Name, t1.Customer_Id, t1.Open_Date, t1.Last_Consulted_Date, t1.Vaccination_Id, t1.Dr_Name, t1.State, t1.Country, t1.DOB, t1.Is_Active  FROM {stage_table} t1 INNER JOIN {country_table} t2 on t1.Customer_Name = t2.Customer_Name WHERE t1.Customer_Id != t2.Customer_Id or t1.Open_Date != t2.Open_Date or t1.Last_Consulted_Date != t2.Last_Consulted_Date or t1.Vaccination_Id != t2.Vaccination_Id or t1.Dr_Name != t2.Dr_Name or t1.State != t2.State or t1.Country != t2.Country or t1.DOB != t2.DOB or t1.Is_Active != t2.Is_Active and t1.Country = '{country_name}'".format(country_table = self.config.countries_lookup[country], stage_table = self.config.stage_tbl,country_name = country)
            return update_query