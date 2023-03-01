# Databricks pipeline example

Folder structure of the Databricks pipeline
The src folder contains the main ETL process that runs on spark cluster in databricks.
The utils folder contains all the helper functions to support connections on data sources and data cleaning.
The databases folder contains the table used to store the data in the corresponding databases
Lastly the test folder contain an example of tests that could be used later on in the project.

The source data used for the demo found here [Hotel Listings 2019](https://www.kaggle.com/datasets/promptcloud/hotel-listings-2019).
With small adjustments other datasets can be used too.


```
BookingsETL
│   README.md
│   requirements.txt
|   requirements_tests.txt
|
└───databses
│   │   gold.sql
|   |   silver.sql
|   |
└───src
│   │   BookingsETL.py
|   |
└───utils
│   │   mount.py
│   │   utils.py
|   |
└───tests
│   │   tests.py
|   |
```
