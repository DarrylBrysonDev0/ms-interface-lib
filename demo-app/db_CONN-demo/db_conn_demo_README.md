# Demo of SQL DB Interface Using db_CONN Class 

## Requirements
- Custom library: microsrv_interface.comm_interface
    - Classes: db_CONN, set_env_param
- pysftp==0.2.9
- pika==1.1.0
- path==14.0.1
- pyodbc==4.0.30
- pandas==1.2.1


```python
import os
import sys
import traceback
import time
import pandas as pd
import json
```


```python
# Custom handeler libray 
# Maintained by Darryl Bryson
# Repo: https://github.com/DarrylBrysonDev0/ms-interface-lib.git

from microsrv_interface.comm_interface import db_CONN, set_env_param
```

Load sample data from csv to dataframe


```python
sample_data_df = pd.read_csv('./data/sample_lab_measurement.csv') 
sample_data_df = sample_data_df.replace('\'', '',regex=True)
sample_data_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>machine_id</th>
      <th>test_id</th>
      <th>technician</th>
      <th>test_routine</th>
      <th>batched</th>
      <th>loc_1_x_offset</th>
      <th>loc_1_y_offset</th>
      <th>loc_1_z_offset</th>
      <th>loc_2_x_offset</th>
      <th>loc_2_y_offset</th>
      <th>loc_2_z_offset</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Machine_13</td>
      <td>15ce86ab99b3490c96ee855c8347e66b</td>
      <td>Jennifer Green</td>
      <td>[d, f, c, b, e]</td>
      <td>NaN</td>
      <td>14.27</td>
      <td>0.295528</td>
      <td>3072</td>
      <td>10.12</td>
      <td>0.953938</td>
      <td>4681</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Machine_23</td>
      <td>efabe31a552649698f04c5e251a88112</td>
      <td>Stephanie Collins</td>
      <td>[a, c, d, e, f, b]</td>
      <td>No</td>
      <td>-4.78</td>
      <td>0.579363</td>
      <td>4453</td>
      <td>0.66</td>
      <td>0.653040</td>
      <td>4747</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Machine_01</td>
      <td>876c727a38274fe59353a76289553a6a</td>
      <td>Ryan Page</td>
      <td>[f, a, d, b, e, c]</td>
      <td>NaN</td>
      <td>13.80</td>
      <td>0.961564</td>
      <td>4824</td>
      <td>-7.30</td>
      <td>0.842194</td>
      <td>4724</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Machine_15</td>
      <td>84f2c6cb19a04fa0ad9dd135ca10b7af</td>
      <td>Willie Golden</td>
      <td>[a, c, e, d]</td>
      <td>Yes</td>
      <td>3.42</td>
      <td>0.838260</td>
      <td>4970</td>
      <td>3.75</td>
      <td>0.466604</td>
      <td>2875</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Machine_20</td>
      <td>76faffd74aab4d189876df0249fb3a87</td>
      <td>Emily Allen</td>
      <td>[b, f]</td>
      <td>Yes</td>
      <td>5.84</td>
      <td>0.499678</td>
      <td>2782</td>
      <td>-12.86</td>
      <td>0.919360</td>
      <td>3112</td>
    </tr>
  </tbody>
</table>
</div>



## Set connection parameters


```python
# Target db table of load
db_tbl_name = '[telem-dev-db].[dbo].[sample_lab_measurement]'
```


```python
# MS-SQL db connection details
res_dic = {}
res_dic['ServerAddr']='192.168.86.33'
res_dic['DBName']='telem-dev-db'
res_dic['UserName']='sa'
res_dic['Password']='Testing1122'

db_configData = json.dumps(res_dic)
```


```python
bufferSize = 1000
```

## db_CONN Utilization
db_CONN Class default configures to environment variables (for docker use cases)
To compensate we reconfigure the instance:
```python
db_interface.configData = db_connStr
db_interface.ingest_buffer_size = bufferSize
db_interface.IS_VERBOSE = True
```
**Parameters**
- configData => MS SQL connection string as JSON object
- ingest_buffer_size => Insert buffer size (Greatly effects overall load time)
- IS_VERBOSE => Get process details


```python
def main(dataDF, db_connStr, targetTBL, bufferSize):
    # Set Database interface
    db_interface = db_CONN()

    try:
        # Connect to Database
        print(' [*] Connecting to database')
        with db_interface as db_conn:
            print(' [+] Connected to database')

            # Load dataframe
            df = dataDF
            
            # Reconfigure connection parameters
            # Default db_CONN configures 
            db_interface.configData = db_connStr
            db_interface.ingest_buffer_size = bufferSize
            db_interface.IS_VERBOSE = True
            
            # Set dataframe property
            db_interface.set_df(df,targetTBL)

            # Write dataframe to linked database table
            row_cnt = db_interface.insert_dataframe()
            print(' [*] Total records writen: {0}'.format(row_cnt))
            print()
            
    except Exception as err:
        print()
        print("An error occurred while executing main proc.")
        print(str(err))
        traceback.print_tb(err.__traceback__)
    return
```


```python
# Execute main method
main(sample_data_df, db_configData, db_tbl_name, bufferSize)
```

     [*] Connecting to database
     [+] Connected to database
     [*] Starting db write
    Connection established
     [+] Buffer reached
     [I] Row index: 1000
     [+] Buffer reached
     [I] Row index: 2000
     [+] Buffer reached
     [I] Row index: 3000
     [+] Buffer reached
     [I] Row index: 4000
     [+] Buffer reached
     [I] Row index: 5000
     [+] Buffer reached
     [I] Row index: 6000
     [+] Buffer reached
     [I] Row index: 7000
     [+] Buffer reached
     [I] Row index: 8000
     [+] Buffer reached
     [I] Row index: 9000
     [+] Buffer reached
     [*] EOD reached
     [I] Row index: 10000
    Results written in: 2.200498580932617
     [*] Total records writen: 10000
    
    
