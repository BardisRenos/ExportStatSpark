# ExportStatSpark

This repository has a combination of Spark, Pandas and working with Excel files and server databases. Working with Spark can handle large volume of data. Also, spark can create some statistical values.



### Using the necessary libraries

In t0he beginning of the code, we first import the necessary libraries. In order to complete the extraction of the data, there is the need to import pandas to create dataframes. The ***pyodbc*** to create a connection to the server database. Regarding Spark, we import spark function, will help us in further. 

```python
  import pandas as pd
  import pyodbc
  from pyspark.sql import *
  from pyspark.sql.functions import *
  from hurry.filesize import size, si
```

### Make the connection to the server database

First the code starts with a new connection to the server database. 
```python
  cnxn = pyodbc.connect(Driver='{SQL Server}', Server='SERVER_NAME', Database='DATABASE_NAME', Trusted_Connection='yes')
```

The server database has a large number of databases. Therefore, the below query retrieves all table names that are below the tables subfolder of the server. Stored to pandas dataframe. Pandas library has the ability to run a sql query and retrieve the results and store them into a dataframe. 

```python
  def the_main_tables(self):
    sqlTables = "SELECT name FROM DATABASE_NAME.sys.tables where name LIKE 'KEY_WORD_PREFIX_%' AND  is_ms_shipped=0 ORDER BY name " \
                "ASC; "

    df = pd.read_sql(sqlTables, self.cnxn)
    return df
```

### Creating the "info" sheet.

This block creates the ***info*** sheet that contains the overall information of all databases. For example, the frist column has the names of the column. The second column has the overall number on database tables. Last, the third column has the actual size in MB. 

```python
  def extraction_main_data(self):
    df = self.the_main_tables()['name']
    for i in df:
        self.dict_Col_Name.setdefault("Col_Name", []).append(i)
        df_re = pd.read_sql("EXEC sp_spaceused  [" + i + "]", self.cnxn)
        self.dict_Size.update({i: df_re['data'].tolist()[0]})
    sum = 0
    for value in self.dict_Size.values():
        sum += int(value.split(" ")[0])

    self.dict_Size.clear()
    self.dict_Names.update({"Application Name": ["E-Project"]})
    self.dict_Size_Tables.update({"# of Tables": [len(df)]})
    self.dict_Size.update({"Size of Table": [size(sum * 1024, si)]})
```


### Configure the Spark environment 

To manage a large volume of database tables, spark can be a useful tool. The first block create the spark session.  

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

The second block has the configuration with the server database with windows authentication.

```python
    for i in self.the_main_tables()['name']:
        mssql_df = spark.read.format("jdbc") \
            .option("url", "jdbc:sqlserver://SERVER_NAME:1433;databaseName=DATABASE_NAME;integratedSecurity=true") \
            .option("dbtable", "[" + i + "]") \
            .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
```

### The exctraction steps.

After making the connection with the database


```python

for k in mssql_df.columns:
    avg_value = str(mssql_df.select(
        avg(length(col(k)))).collect()[0][0])
    self.dict_AVG_Value.setdefault(i, []).append(
        avg_value.split(".")[0])

    min_value = mssql_df.select(
        min(length(col(k)))).collect()[0][0]
    self.dict_MIN_value.setdefault(i, []).append(min_value)

    max_value = mssql_df.select(
        max(length(col(k)))).collect()[0][0]
    self.dict_MAX_Value.setdefault(i, []).append(max_value)

    null_value = mssql_df.where(col(k).isNull()).count()
    self.dict_Null_Value.setdefault(i, []).append(null_value)

    empty_value = mssql_df.where(length(col(k)) == 0).count()
    self.dict_Empty_Value.setdefault(i, []).append(empty_value)

    total_valid_values = mssql_df.count() - (empty_value + null_value)
    self.dict_Total_Valid_Value.setdefault(
        i, []).append(total_valid_values)

    percentage_completion = (
        (total_valid_values / mssql_df.count()) * 100) if mssql_df.count() != 0 else 0
    self.dict_Percentage_Comp.setdefault(
        i, []).append(int(percentage_completion))

    type_value = self.lis_of_types[k]
    self.dict_Category_Of_data.setdefault(i, []).append(type_value)

    self.dict_Total_Length_Values.setdefault(
        i, []).append(mssql_df.count())
    self.dict_Total_Length_Col_Values.setdefault(
        i, []).append(len(mssql_df.columns))

    print(i, k, mssql_df.count(), avg_value, min_value, max_value, null_value, empty_value,
          total_valid_values, percentage_completion, self.lis_of_types[k])

```









