# Getting started

## Script list

----

1. [CSVToYAML_ManagedToExternal.ipynb](./CSVToYAML_ManagedToExternal.ipynb) This script will convert csv file into YAML file which will need for the other processes
**specifications for the script to run as below**

- Required libraries
    <ol>
    <li><a href ="https://pypi.org/project/pandas/" target=”_blank”> pandas </a></li>
    <li><a href ="https://pypi.org/project/PyYAML/" target=”_blank”>PyYAML</a></li>
    </ol>

2. CSV field requirements

    <ol>
    <li> <b>database</b> : This field is the schema name in hive metastore</li>
    <li> <b>name</b> : This is the table name in the above mentioned schema</li>
    <li> <b>target_schema</b> : Scema that need to be upgraded into</li>
    <li> <b>is_pii</b> : the status of the given table is pii arguments here is "true" or "false"</li>
    </ol>

3. Once the CSV is generated you can put the path of the csv into the "csvpath" parameter and YAML file will be available in "yamlpath" that you have mentioned in above notebook.