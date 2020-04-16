# ExportStatSpark

This repository has a combination of Spark, Pandas and working with Excel files and server databases. Working with Spark can handle large volume of data. Also, spark can create some statistical values.



### Using the necessary libraries

In t0he beginning of the code, we first import the necessary libraries. In order to complete the extraction of the data, there is the need to import pandas to create dataframes. The ***pyodbc*** creates a connection to the server database. Regarding Spark, we import spark function, will help us in further. 

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

The second block has the configuration with the server database with windows authentication. This code loops on every table name and each one of them is stored into ***mssql_df***. 

```python
    for i in self.the_main_tables()['name']:
        mssql_df = spark.read.format("jdbc") \
            .option("url", "jdbc:sqlserver://SERVER_NAME:1433;databaseName=DATABASE_NAME;integratedSecurity=true") \
            .option("dbtable", "[" + i + "]") \
            .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
```

### The exctraction steps.

After making the connection with the databases. The algorithm, applies 8 elements of data stat. For example, the code extracts the average length value (***avg_value***), the minimum length value (***min_value***), the maximum length value (***max_value***), the number of NULL values that a column may contains (***null_values***). Moreover, the number of empty values (***empty_value***) which a column may contains. This method also contains the total valid values of each column (***total_valid_values***). Is followed by the percentage of completion, which depicts how much a column is fulfil. Another feature is the type of the column and lastly, the length of the column.

```python
  for k in mssql_df.columns:
  
    avg_value = str(mssql_df.select(avg(length(col(k)))).collect()[0][0])
    self.dict_AVG_Value.setdefault(i, []).append(avg_value.split(".")[0])

    min_value = mssql_df.select(min(length(col(k)))).collect()[0][0]
    self.dict_MIN_value.setdefault(i, []).append(min_value)

    max_value = mssql_df.select(max(length(col(k)))).collect()[0][0]
    self.dict_MAX_Value.setdefault(i, []).append(max_value)

    null_value = mssql_df.where(col(k).isNull()).count()
    self.dict_Null_Value.setdefault(i, []).append(null_value)

    empty_value = mssql_df.where(length(col(k)) == 0).count()
    self.dict_Empty_Value.setdefault(i, []).append(empty_value)

    total_valid_values = mssql_df.count() - (empty_value + null_value)
    self.dict_Total_Valid_Value.setdefault(i, []).append(total_valid_values)

    percentage_completion = ((total_valid_values / mssql_df.count()) * 100) if mssql_df.count() != 0 else 0
    self.dict_Percentage_Comp.setdefault(i, []).append(int(percentage_completion))

    type_value = self.lis_of_types[k]
    self.dict_Category_Of_data.setdefault(i, []).append(type_value)

    self.dict_Total_Length_Values.setdefault(i, []).append(mssql_df.count())
    self.dict_Total_Length_Col_Values.setdefault(i, []).append(len(mssql_df.columns))

    print(i, k, mssql_df.count(), avg_value, min_value, max_value, null_value, empty_value,
          total_valid_values, percentage_completion, self.lis_of_types[k])
```


### The data exportation into excel sheet

This bloc shows how the data from the above code bloc.  

```python

def file_export(self, app_name, num_tables, table_size, dict_col_name, dict_avg_value, dict_min_value, dict_max_value,
                    dict_null_value, dict_empty_value, dict_total_valid_value, dict_percentage_comp, dict_category_of_data,
                    dict_total_values, dict_total_columns_values):

    self.extract_data()
    
# The dictAll (dictionary type) contains the three main statistical data. 

    dictAll = {}
    dictAll.update(app_name)
    dictAll.update(num_tables)
    dictAll.update(table_size)

# Using the ExcelWriter method from Pandas library it is possible to export a dataframe into excel file. The three previous dictionaries # are merge into a single one and then exported into an axcel file.  

    writer = pd.ExcelWriter(r'C:\Users\user\Desktop\project\file_name.xlsx')
    dfTotal = pd.DataFrame(data=dictAll)
    dfTotal.to_excel(writer, index=None, header=True, sheet_name="Info")

    header = ["Column_Name", "AVG_Value", "Min_Value", "Max_Value", "Null_Fields",
              "Empty_Fields", "Total_Valid_Values", "Percentage_Of_Completion", "Data_Type",
              "Row_Length", "Column_Size"]

    for key, value in dict_col_name.items():
        dfdictColName = pd.DataFrame(data=value, columns=[header[0]])
        dfdictColName.to_excel(writer, sheet_name=key, startcol=0, header=True, index=False)

    for key, value in dict_avg_value.items():
        dfdictAvgValue = pd.DataFrame(data=value, columns=[header[1]])
        dfdictAvgValue.to_excel(writer, sheet_name=key, startcol=1, header=True, index=False)

    for key, value in dict_min_value.items():
        dfdictMinValue = pd.DataFrame(data=value, columns=[header[2]])
        dfdictMinValue.to_excel(writer, sheet_name=key, startcol=2, header=True, index=False)

    for key, value in dict_max_value.items():
        dfdictMaxValue = pd.DataFrame(data=value, columns=[header[3]])
        dfdictMaxValue.to_excel(writer, sheet_name=key, startcol=3, header=True, index=False)

    for key, value in dict_null_value.items():
        dfdictNullValue = pd.DataFrame(data=value, columns=[header[4]])
        dfdictNullValue.to_excel(writer, sheet_name=key, startcol=4, header=True, index=False)

    for key, value in dict_empty_value.items():
        dfdictEmptyValue = pd.DataFrame(data=value, columns=[header[5]])
        dfdictEmptyValue.to_excel(writer, sheet_name=key, startcol=5, header=True, index=False)

    for key, value in dict_total_valid_value.items():
        dfdictTotalValidValue = pd.DataFrame(data=value, columns=[header[6]])
        dfdictTotalValidValue.to_excel(writer, sheet_name=key, startcol=6, header=True, index=False)

    for key, value in dict_percentage_comp.items():
        dfdictMinValue = pd.DataFrame(data=value, columns=[header[7]])
        dfdictMinValue.to_excel(writer, sheet_name=key, startcol=7, header=True, index=False)

    for key, value in dict_category_of_data.items():
        dfdictMinValue = pd.DataFrame(data=value, columns=[header[8]])
        dfdictMinValue.to_excel(writer, sheet_name=key, startcol=8, header=True, index=False)

    for key, value in dict_total_values.items():
        dfdictMinValue = pd.DataFrame(data=value, columns=[header[9]])
        dfdictMinValue.to_excel(writer, sheet_name=key, startcol=9, header=True, index=False)

    for key, value in dict_total_columns_values.items():
        dfdictMinValue = pd.DataFrame(data=value, columns=[header[10]])
        dfdictMinValue.to_excel(writer, sheet_name=key, startcol=10, header=True, index=False)

    writer.close()

```






