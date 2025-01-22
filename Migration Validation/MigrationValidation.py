# Databricks notebook source
# MAGIC %md
# MAGIC ## Validation script post migrating hive_metastore to UC
# MAGIC
# MAGIC #### Overview
# MAGIC Crosschecks tables in hive_metastore (based on UCX assesment) and tables in UC (based on 'system.information_schema'). This script should be run in interactive mode and needs a person to check it's results.
# MAGIC
# MAGIC #### Pre-requisites
# MAGIC Run UCX assesment right after the migration script and before running this validation. This will ensure all existent & migrated tables are captured in the UCX tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

# Widgets
dbutils.widgets.text("ucx-schema", "ucx", "UCX schema in hive_metastore")
dbutils.widgets.text("exclusion-schemas", "ucx", "Schemas to exclude from this validation")
dbutils.widgets.text("uc-catalog", "", "UC catalog")

# COMMAND ----------

ucx_schema = dbutils.widgets.get("ucx-schema")
uc_catalog = dbutils.widgets.get("uc-catalog")
exclusion_schemas = dbutils.widgets.get("exclusion-schemas")
if (exclusion_schemas != ''):
    exclusion_schemas_list = (str(exclusion_schemas.replace(' ','').split(',')).replace("[", "(").replace("]", ")"))
if not ucx_schema:
    raise ValueError("You need specify the name of the UCX schema.")
if not uc_catalog:
    raise ValueError("You need to specify the name of the UC catalog.")


# COMMAND ----------

# Inspect external tables in Hive_metastore; number should match the total count of migrated tables in the migration script
display(spark.sql(f"select * from hive_metastore.{ucx_schema}.tables where object_type == 'EXTERNAL' and database not in {exclusion_schemas_list}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables in UC not marked as upgraded

# COMMAND ----------

# external tables from hive which have not been upgraded; there should be no entries here; if there are, they should be looked at row by row and added to an issue list; ensure the UCX assesment has been run after the hive migration to this list is up to date
display(spark.sql(f"select * from hive_metastore.{ucx_schema}.tables where object_type == 'EXTERNAL' and database not in {exclusion_schemas_list} and upgraded_to is null"))

# COMMAND ----------

# inspect all tables in UC
display(spark.sql(f"select table_catalog, table_schema, table_name, table_type from {uc_catalog}.information_schema.tables where table_schema <> 'information_schema' and table_type = 'EXTERNAL'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Difference between the tables in hive and the ones in UC

# COMMAND ----------

hive_tables = spark.sql(f"select database as table_schema, name as table_name from hive_metastore.{ucx_schema}.tables where object_type == 'EXTERNAL' and database not in {exclusion_schemas_list}")

uc_tables = spark.sql(f"select table_schema, table_name from {uc_catalog}.information_schema.tables where table_schema <> 'information_schema' and table_type = 'EXTERNAL'")

# tables present in hive but not in UC; the resulting difference should be empty, there should be no entries here; if there are, they should be looked at row by row and added to an issue list
print("Tables in Hive but not in UC:")
display(hive_tables.exceptAll(uc_tables))

# tables present in UC but not in hive; the resulting difference should be empty, there should be no entries here; if there are, they should be looked at row by row and added to an issue list
print("Tables in UC but not in Hive:")
display(uc_tables.exceptAll(hive_tables))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Difference between the views in hive and the ones in UC

# COMMAND ----------

# do the difference between the views in hive and the ones in UC
hive_views = spark.sql(f"select database as table_schema, name as table_name from hive_metastore.{ucx_schema}.tables where object_type == 'VIEW' and database not in {exclusion_schemas_list}")
uc_views = spark.sql(f"select table_schema, table_name from {uc_catalog}.information_schema.tables where table_schema <> 'information_schema' and table_type = 'VIEW'")

# views present in hive but not in UC; the resulting difference should be empty, there should be no entries here; if there are, they should be looked at row by row and added to an issue list
print("Views in Hive but not in UC:")
display(hive_views.exceptAll(uc_views))

# views present in UC but not in hive; the resulting difference should be empty, there should be no entries here; if there are, they should be looked at row by row and added to an issue list
print("Views in UC but not in Hive:")
display(uc_views.exceptAll(hive_views))
