# Databricks notebook source
# ## Create Widgets
# dbutils.widgets.removeAll()
# dbutils.widgets.text("remount", "") 
# dbutils.widgets.dropdown("is_drop_managed_tables", "False", ["True", "False"], "Drop Managed Tables?")
# dbutils.widgets.text("external_table_pii_location", "abfss://", "External Storage (PII)")
# dbutils.widgets.text("external_table_nonpii_location", "abfss://", "External Storage (Non-PII)")
# dbutils.widgets.text("yaml_configuration", "", "YAML table list")

# COMMAND ----------

## Configure logging
import sys
import logging

level = logging.INFO
logging.basicConfig(stream=sys.stderr,level=level,format="%(asctime)s [%(name)s][%(levelname)s] %(message)s")
logging.getLogger("databricks.sdk").setLevel(level)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)
logger = logging.getLogger()

# COMMAND ----------

## Set Widgets
external_table_pii_location = dbutils.widgets.get("external_table_pii_location")

external_table_nonpii_location = dbutils.widgets.get("external_table_nonpii_location")

is_drop_managed_tables = dbutils.widgets.get("is_drop_managed_tables")

yaml_configuration = dbutils.widgets.get("yaml_configuration")

# COMMAND ----------

pip install pandas

# COMMAND ----------

pip install PyYAML

# COMMAND ----------

import yaml
import pandas as pd
import datetime
import time
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit
pd.set_option("display.max_rows", None) 

class HMSTableMigration:
    '''
        Contains a list of utility methods to migrate managed tables to external tables within hive_metastore.
    '''

    def __init__(self, spark, yaml_configuration):
        self.spark = spark
        self.yaml_configuration = yaml_configuration
        self.managed_table_list = pd.DataFrame()
        self.managed_to_external_failed_tables = []
        self.converted_tables_list = []

        
    def _load_migration_conf(self, yaml_configuration,root="resources",leaf="tables"):
        logger.info(f"yaml file {yaml_configuration}")
        # Load all migration tables yaml
        with open(yaml_configuration, 'r') as file:
            config = yaml.safe_load(file)
            if(len(yaml_configuration) > 0):
                if bool(config):
                    try:
                        pdf = pd.DataFrame.from_dict(config[root][leaf],orient="index").sort_values(by='t_no')
                        display(pdf)
                        if leaf == "tables":
                            self.managed_table_list = pdf   
                    except Exception as exception:
                        logger.info("Cant find any excluded tables in the list given or not in correct format")
                        raise
        
        return self.managed_table_list,config

    def check_point(self,df):
        df.coalesce(1).write.format("csv").option("header", "true").save("checkpoint.csv")

    def check_table_exist(self,table_name):
        return spark.catalog.tableExists(f"{table_name}")    
    
    def check_table_is_managed(self,database_name,managed_table,catalog="hive_metastore"):
        """Table is External return False
           Table is Managed return True
        """
        if self.check_table_exist(f"`{catalog}`.`{database_name}`.`{managed_table}`"):
            val = ""
            df_managed = spark.sql(f"""DESCRIBE TABLE EXTENDED
                                    {catalog}.`{database_name}`.`{managed_table}`""").where("col_name = 'Type'").collect()
            for row in df_managed:
                val = row["data_type"]

            if str(val).lower() ==  "managed":
                return True
            else:
                False
        return False
    
    def assert_managed_vs_external(self, database_name, managed_table):
        # Register Managed Table &. get count
        managed_table = spark.table(f"{database_name}.{managed_table}_old")
        managed_table_count = managed_table.count()

        # Register External Table & get count
        external_table = spark.table(f"{database_name}.{managed_table}")
        external_table_count = external_table.count()

        # Assert both are matching
        assert managed_table_count == external_table_count


    # -------------------------------
    #    Migrate MANAGED Tables
    # -------------------------------

    def upgrade_hm_managed_to_hm_external(self, database_name, managed_table, location,table_number):
        '''
            Upgrades the Managed Table to the External Table within Hive Metastore

            Parameters:
                database_name:          Name of the hive metastore database where table resides
                new_database_name:      Name of the unity database where table will be recreated
                managed_table:          The full table name to be converted
                external_table:         The new full table after conversion
                location:               The destination where the table will be recreated based on pii flag.
        '''
        

        # Use the provided HM Database
        spark.sql(f"USE {database_name}")
        tablesize_in_mbs = 0
        logger.info(f"Transfer started : {database_name}.{managed_table}")
        try:
            #Timer Started
            start_time = datetime.datetime.now()
            if self.check_table_exist(f"`hive_metastore`.{database_name}.{managed_table}_ext"):
                logger.info(f"External table {managed_table}_ext already exist")
                logger.info(f"Transfer skipped : {database_name}.{managed_table}")

            else:   
                if self.check_table_is_managed(database_name,f"{managed_table}"):
                    print(f"""
                        CREATE TABLE IF NOT EXISTS {managed_table}_ext
                        DEEP CLONE {managed_table} 
                        LOCATION '{location}'
                    """)
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {managed_table}_ext
                        DEEP CLONE {managed_table} 
                        LOCATION '{location}'
                    """)
                    
                    for i in spark.sql(f"describe detail `hive_metastore`.{database_name}.{managed_table}").select("sizeInBytes").collect():
                        tablesize_in_mbs = (i["sizeInBytes"]/(1000*1000))
                    try:
                        if not self.check_table_exist(f"`hive_metastore`.{database_name}.{managed_table}_old") and self.check_table_is_managed(database_name,f"{managed_table}"):
                            spark.sql(f"ALTER TABLE {managed_table} RENAME TO {managed_table}_old")
                            logger.info(f"Renaming completed! Managed table is {managed_table}_old")
                        else:
                            logger.info(f"Table :{database_name}.{managed_table}_old already exists")    
                    except Exception as exception:
                        full_table_name_source = f"{database_name}.{managed_table}"
                        logger.info(f"Failed to rename the managed table :{database_name}.{managed_table} due to: {str(exception)}")
                        self.managed_to_external_failed_tables.append((full_table_name_source,table_number,str(exception)))
                        pass
                    try:
                        if  self.check_table_exist(f"`hive_metastore`.{database_name}.{managed_table}_ext") and not self.check_table_is_managed(database_name,f"{managed_table}_ext"):
                            spark.sql(f"ALTER TABLE {managed_table}_ext RENAME TO {managed_table}")
                            logger.info(f"Renaming completed! External table is {managed_table}")
                        else:
                            logger.info(f"Table :{database_name}.{managed_table}_ext does not exists") 
                    except Exception as exception:
                        logger.info(f"Failed to rename the external table :{database_name}.{managed_table}_ext due to: {str(exception)}")
                        self.managed_to_external_failed_tables.append((full_table_name_source,table_number,str(exception)))
                        pass
                    end_time = datetime.datetime.now()
                    duration = end_time - start_time
                    duration_in_s = duration.seconds
                    m, s = divmod(duration_in_s, 60)
                    m = int(m)
                    s = int(s) 
                    rate = 0.0
                    if duration_in_s != 0:
                        rate = tablesize_in_mbs/duration_in_s             
                    self.converted_tables_list.append((database_name,managed_table,f'{m:02d}:{s:02d}',tablesize_in_mbs,rate))        

        except Exception as exception:
            logger.info(f"Failed to clone managed table {database_name}.{managed_table} due to: {str(exception)}")
            full_table_name_source = f"{database_name}.{managed_table}"
            self.managed_to_external_failed_tables.append((full_table_name_source,table_number,str(exception)))
            pass
        

        # Assert the count
        # assert_managed_vs_external(managed_table)
        if(is_drop_managed_tables == "True" and self.check_table_is_managed(database_name,f"{managed_table}_old")):
            spark.sql(f"DROP TABLE {managed_table}_old")
            logger.info(f"Dropping completed! Managed table removed {managed_table}_old")

    def execute(self):
        # Load the configuration file
        mtl,config = self._load_migration_conf(yaml_configuration)

        # Loop through each table and run the conversion
        # for table in config['resources']['tables']:
        for index, row in mtl.iterrows():    
            # managed_table_obj = config['resources']['tables'][table]
            database_name = row['database']
            managed_table = row['name']
            external_table = row['target_table']
            is_pii = row['is_pii']
            new_database_name = row['target_schema']
            table_number = row['t_no']

            location = (lambda is_pii, external_table_nonpii_location, external_table_pii_location, managed_table: 
                f"{external_table_pii_location if is_pii else external_table_nonpii_location}/{new_database_name}/{external_table}"
                )(is_pii, external_table_nonpii_location, external_table_pii_location, managed_table)
            
            self.upgrade_hm_managed_to_hm_external(database_name, managed_table, location,table_number)

    #Getting the status of the migration        
    def printMigrationStats(self):
        '''
            Prints migration statistics.
        '''
        logger.info("CONVERSION RESULTS")
        logger.info("-------------------------------------------------------------------")
        logger.info(f"TABLES CANNOT CONVERT TO EXTERNAL: {len(self.managed_to_external_failed_tables)}")
        if (len(self.managed_to_external_failed_tables) > 0):
            all_not_converted = self.managed_to_external_failed_tables
            non_dbfs_root = [x for x in all_not_converted]
            if (len(non_dbfs_root) > 0):
                display(non_dbfs_root)

        logger.info(f"TABLES CONVERTED TO EXTERNAL: {len(self.converted_tables_list)}") 
        if (len(self.converted_tables_list ) > 0):
            all_not_converted = self.converted_tables_list
            non_dbfs_root = [x for x in all_not_converted]
            columns = ["Target Schema","Target Table Name","Time elapsed to convert","Table size (Mb)","Transfer speed(MbPS)"]
            if (len(non_dbfs_root) > 0):
                sparkdf = spark.createDataFrame(non_dbfs_root,columns)
                display(sparkdf)          

# COMMAND ----------

internal_migration = HMSTableMigration(spark, yaml_configuration)
internal_migration.execute()
internal_migration.printMigrationStats()
