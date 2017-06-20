'''@author yuanzi'''

import pymysql
import os

def connectDB(username,password):
    connection = pymysql.connect(host='localhost',user=username,password=password,database='Project',local_infile=True)
    return connection

def getGivenFileName(path,table_name):
    given_name_list = []
    namelist = os.listdir(path)
    for i in range(len(namelist)):
        if namelist[i].find(table_name) == 0:
            given_name_list.append(namelist[i])
    return given_name_list

path = '/Users/yuanzi/Downloads/data2'
key = 'lineitem.tbl'
table = 'LINEITEM'
username = 'root'
password = '5647477230'


given_name_list = getGivenFileName(path,key)
connection = connectDB(username,password)
cursor = connection.cursor()
for i in range(len(given_name_list)):
    sql = 'LOAD DATA LOCAL INFILE \''+path+'/'+given_name_list[i]+'\' INTO TABLE '+table+' FIELDS TERMINATED BY \'|\' LINES TERMINATED BY \'|\n\''
    r = cursor.execute(sql)
    print('已导入表：'+given_name_list[i]+'一共'+str(r)+'条数据')
    connection.commit()
    print(sql)
connection.close()