# Databricks notebook source
pip install PyYAML

# COMMAND ----------

pip install pandas

# COMMAND ----------

dbutils.widgets.text("yaml_configuration", "", "YAML table list")
dbutils.widgets.text("Transfer_Speed", "", "Transfer speed of the table MbPS")

# COMMAND ----------

yaml_configuration = dbutils.widgets.get("yaml_configuration")
Transfer_Speed = dbutils.widgets.get("Transfer_Speed")

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

import yaml
import pandas as pd
import datetime
import time
from decimal import Decimal
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit
pd.set_option("display.max_rows", None) 

class HMSTimeEstimate:
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

    # -------------------------------
    #    Migrate MANAGED Tables
    # -------------------------------

    def hm_managed_to_hm_external_estimate(self, database_name, managed_table,table_number):
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
        rate = 0.0
        logger.info(f"Estimate Started for Table : {database_name}.{managed_table}")

        if not self.check_table_exist(f"`hive_metastore`.`{database_name}`.`{managed_table}`"):
                logger.info(f"Estimation skipped because table not exist : {database_name}.{managed_table}")

        else:
          try:

            if self.check_table_is_managed(database_name,f"{managed_table}"):
      
              for i in spark.sql(f"describe detail `hive_metastore`.{database_name}.{managed_table}").select("sizeInBytes").collect():
                  tablesize_in_mbs = (i["sizeInBytes"]/(1000*1000))

              duration_in_s = tablesize_in_mbs * float(Transfer_Speed)
              m,s = divmod(duration_in_s, 60)
              m = int(m)
              s = int(s)
              self.converted_tables_list.append((database_name,managed_table,tablesize_in_mbs,f'{m:02d}:{s:02d}',duration_in_s)) 
              logger.info(f"Estimate Ended for Table : {database_name}.{managed_table}")       

          except Exception as exception:
              logger.info(f"Failed to clone managed table {database_name}.{managed_table} due to: {str(exception)}")
              full_table_name_source = f"{database_name}.{managed_table}"
              self.managed_to_external_failed_tables.append((full_table_name_source,table_number,str(exception)))
              pass
        

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
            
            self.hm_managed_to_hm_external_estimate(database_name, managed_table,table_number)

    #Getting the status of the migration        
    def printMigrationStats(self):
        '''
            Prints migration statistics.
        '''

        logger.info(f"EVALATED TABLES : {len(self.converted_tables_list)}") 
        if (len(self.converted_tables_list ) > 0):
            all_not_converted = self.converted_tables_list
            non_dbfs_root = [x for x in all_not_converted]
            columns = ["Target Schema","Target Table Name","Table size (Mb)","Estiamted Elapsed time to convert","Estiamted Elapsed time to convert in seconds"]
            if (len(non_dbfs_root) > 0):
                sparkdf = spark.createDataFrame(non_dbfs_root,columns)
                aggrigatedval = sparkdf.select('Estiamted Elapsed time to convert in seconds').agg({'Estiamted Elapsed time to convert in seconds': 'sum'})
                aggrigatedval= aggrigatedval.withColumn('Estiamted Elapsed time to convert in Minutes', aggrigatedval["sum(Estiamted Elapsed time to convert in seconds)"]/60).withColumn('Estiamted Elapsed time to convert in Hours', aggrigatedval["sum(Estiamted Elapsed time to convert in seconds)"]/(60*60))
                display(sparkdf)
                display(aggrigatedval)     

# COMMAND ----------

internal_migration = HMSTimeEstimate(spark, yaml_configuration)
internal_migration.execute()
internal_migration.printMigrationStats()
