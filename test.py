import logging, sys
import pandas as pd
from pyspark import SparkContext

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)

sc = SparkContext()

dir1 = '/gpfs01/hadoop/user/cng10/Erick/zillow/stores/'
# dir2 = dir1 + '02/'

layoutZAsmt = pd.read_excel(dir1+'Layout.xlsx', sheetname=0)
layoutZTrans = pd.read_excel(dir1+'Layout.xlsx', sheetname=1)

logging.info('Finish loading layout')

col_namesMain = layoutZAsmt[layoutZAsmt['TableName'] == 'utMain', 'FieldName'].T
col_namesBldg = layoutZAsmt[layoutZAsmt['TableName'] == 'utBuilding', 'FieldName'].T
col_namesBldgA = layoutZAsmt[layoutZAsmt['TableName'] == 'utBuildingAreas', 'FieldName'].T
