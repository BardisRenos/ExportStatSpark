# ExportStatSpark

This repository has a combination of Spark, Pandas and working with Excel files and server databases. Working with Spark can handle large volume of data. Also, spark can create some statistical values.



### Using the necessary libraries

At the beginning of the code, we first import the necessary libraries. In order to complete the extraction of the data, there is the need to import pandas to create dataframes. The ***pyodbc*** creates a connection to the server database. Regarding Spark, we import spark function, will help us in further. 

```python
  import pandas as pd
  import pyodbc
  from pyspark.sql import *
  from pyspark.sql.functions import *
  from hurry.filesize import size, si
```

### Create a pipeline to the server database

First the code starts with a new connection to the server database. Create=ing a pipeline with pyodbc library to a database server by giving the parameters and indicating that the connection is trusted. 

```python
  cnxn = pyodbc.connect(Driver='{SQL Server}', Server='SERVER_NAME', Database='DATABASE_NAME', Trusted_Connection='yes')
```

One server has normaly a large number of databases. Therefore, the below query retrieves all table names (only) that are below the table's sub folder of the server. Stored to a pandas dataframe. Pandas library has the ability to run a sql query and retrieve the results and store them into a dataframe format. 

```python
  def the_main_tables(self):
    sqlTables = "SELECT name FROM DATABASE_NAME.sys.tables where name LIKE 'KEY_WORD_PREFIX_%' AND  is_ms_shipped=0 ORDER BY name " \
                "ASC; "

    df = pd.read_sql(sqlTables, self.cnxn)
    return df
```

### Configure the Spark environment 

To manage a large volume of database tables, Spark can be a useful tool. The first block create the spark session to my local machine.  

```python
 def extract_data(self):
    spark = SparkSession \
          .builder \
          .master("local") \
          .appName("Python Spark SQL basic example") \
          .config("spark.driver.extraClassPath",
                  "C:\\Users\\renos.bardis\\PycharmProjects\\erp-mdm\\mssql-jdbc-7.4.1.jre8.jar:C:\\Users"
                  "\\renos.bardis\\PycharmProjects\\erp-mdm\\sqljdbc_auth.dll") \
          .config("spark.debug.maxToStringFields", 2000) \
          .getOrCreate()
```

The second block has the configuration with the server database with windows authentication. This code loops over the previous dataframe ***sqlTables***. Every table name is stored into ***mssql_df***. The below code is configured to connect to jdbc (java database connector). Also, giving the sql server and the port for the connection. 

```python
    for i in self.the_main_tables()['name']:
        mssql_df = spark.read.format("jdbc") \
            .option("url", "jdbc:sqlserver://SERVER_NAME:1433;databaseName=DATABASE_NAME;integratedSecurity=true") \
            .option("dbtable", "[" + i + "]") \
            .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
```

### The exctraction steps.

After making the connection with the databases. The algorithm, applies 7 elements of data stat. For example, the code extracts the average length value (***avg_value***), the minimum length value (***min_value***), the maximum length value (***max_value***), the number of NULL values that a column may contains (***null_values***). Moreover, the number of empty values (***empty_value***) which a column may contains. This method also contains the total valid values of each column (***total_valid_values***). Is followed by the ***percentage of completion***, which depicts how much a column is fulfil. Another feature is the type of the column and lastly, the length of the column.

Here the code starts with the loop on top of all columns. By each column the code calculates and stores into a dictionary. For example, in the first block, calculates the average length character of the first column. After that, stored it into a key value storage.  

```python
  for k in mssql_df.columns:
  
    avg_value = str(mssql_df.select(avg(length(col(k)))).collect()[0][0])
    self.dict_AVG_Value.setdefault(i, []).append(avg_value.split(".")[0])
```
In the second block. The code calculates the minimum length character of the same column.

```python
    min_value = mssql_df.select(min(length(col(k)))).collect()[0][0]
    self.dict_MIN_value.setdefault(i, []).append(min_value)
```

In followed block, the code calculates the maximum length character of the aforementioned column. 

```python
    max_value = mssql_df.select(max(length(col(k)))).collect()[0][0]
    self.dict_MAX_Value.setdefault(i, []).append(max_value)
```

Counts how many cells has null values.

```python
    null_value = mssql_df.where(col(k).isNull()).count()
    self.dict_Null_Value.setdefault(i, []).append(null_value)
```

Also, the code counts the empty fields.

```python
    empty_value = mssql_df.where(length(col(k)) == 0).count()
    self.dict_Empty_Value.setdefault(i, []).append(empty_value)
```

Here the code counts number of cells of the column that are not empty or null.

```python
    total_valid_values = mssql_df.count() - (empty_value + null_value)
    self.dict_Total_Valid_Value.setdefault(i, []).append(total_valid_values)
```

Calculates the percentage of the valid values (non null values or empty ones)

```python
    percentage_completion = ((total_valid_values / mssql_df.count()) * 100) if mssql_df.count() != 0 else 0
    self.dict_Percentage_Comp.setdefault(i, []).append(int(percentage_completion))
```

```python
    type_value = self.lis_of_types[k]
    self.dict_Category_Of_data.setdefault(i, []).append(type_value)
```

```python
    self.dict_Total_Length_Values.setdefault(i, []).append(mssql_df.count())
    self.dict_Total_Length_Col_Values.setdefault(i, []).append(len(mssql_df.columns))
```






