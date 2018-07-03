import logging
import sys
import pandas as pd
from pyspark.sql import SparkSession

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

dir1 = '/gpfs01/hadoop/user/cng10/Erick/zillow/stores/'
dir2 = dir1 + '02/'

layoutZAsmt = pd.read_excel(dir1+'Layout.xlsx', sheet_name=0)
layoutZTrans = pd.read_excel(dir1+'Layout.xlsx', sheet_name=1)

logging.info('Finish loading layout')

col_namesMain = layoutZAsmt.loc[layoutZAsmt['TableName'] == 'utMain', 'FieldName'].T
col_namesBldg = layoutZAsmt.loc[layoutZAsmt['TableName'] == 'utBuilding', 'FieldName'].T
col_namesBldgA = layoutZAsmt.loc[layoutZAsmt['TableName'] == 'utBuildingAreas', 'FieldName'].T

base = pd.read_table(dir2+"ZAsmt/Main.txt", sep='|', header=None, low_memory=False, names=col_namesMain)

print(base.head())
print(base.info())

base1 = spark.createDataFrame(base)

print(type(base1))
