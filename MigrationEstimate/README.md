# Getting started

## Script list

----

1. [HMSTimeEstimate.py](./HMSTimeEstimate.py) This script will estimate the time that is taking for converting manged tables to external tables
**specifications for the script to run as below**
    - For a cluster config as below, the transfer-speed is around 5.6 Mbps
        <ul>
        <li>Worker type - 128 Gb memory, 32 Cores , 2 min workers 20 max workers</li>
        <li>Driver type - 128 Gb memory, 16 Cores </li>
        </ul>
    - There will be 2 parameters in the script <br>

    ```
    Transfer_speed - This will be 5.6 on above specified cluster configuration
    yaml_configuration - This will be the path to YAML file which contain manged tables
    ```

    - Required libraries
        <ol>
        <li><a href ="https://pypi.org/project/pandas/" target=”_blank”> pandas </a></li>
        <li><a href ="https://pypi.org/project/PyYAML/" target=”_blank”>PyYAML</a></li>
        </ol>
