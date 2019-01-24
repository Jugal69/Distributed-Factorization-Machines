from pyspark.sql import SparkSession
import numpy as np
import copy
from pyspark import SparkContext,SparkConf

def RDD_to_list(lines):
    RDDlist=[]
    for i in range(len(lines)):
        if ':' in lines[i]:
            movieID = lines[i].replace(':', '')
        else:
            temp=lines[i].split(',')
            temp.extend(temp[2].split('-'))
            del temp[2]
            temp.insert(0,int(movieID))
            RDDlist.append(temp)
    return RDDlist

def calculateMonths(line):
    return [int(line[1]),(line[0],int(line[2]),(int(line[3])-start_date)*12+int(line[4])+int(line[5])/30.0)]

def findLastMovie(x):
    diff=np.zeros(len(x[1]))
    movie=np.zeros(len(x[1]))
    output=[]
    for i in range(len(x[1])):
        movie[i]=x[1][i][1][0]
        diff[i]=x[1][i][1][2]
    for i in range(len(x[1])):
        d=copy.deepcopy(diff)
        d[i]=10*diff[i]
        last_movie=-1 if x[1][i][1][2]==np.min(diff) else movie[np.argmin(abs(d-x[1][i][1][2]))]
        output.append([x[1][i][1][1],x[0],movie[i],x[1][i][0],diff[i],last_movie])
    return output

def toLibfm(data):
    libfm=[str(data[0]),str(data[1]-1)+':1',str(int(user-1+data[2]))+':1']
    for i in range(len(data[3][0])):
        libfm.append(str(user-1+movies+data[3][0][i])+':'+str(data[3][1]))
    libfm.append(str(user+2*movies)+':'+str(data[4]))
    if data[5]!=-1:
        libfm.append(str(int(user+2*movies+data[5]))+':1')
    return ' '.join(i for i in libfm)

conf=SparkConf().setAppName('FM').setMaster(master)
sc=SparkContext(conf=conf)

directory='file://~/Project/netflix-prize-data/'
lines=sc.textFile(directory+'combined_data_1.txt')
RDD_data=sc.parallelize(RDD_to_list(lines.collect()))
for i in range(2,5):
    lines=sc.textFile(directory+'combined_data_'+str(i)+'.txt')
    RDD_data=sc.union([RDD_data,sc.parallelize(RDD_to_list(lines.collect()))])

movies=RDD_data.map(lambda x:x[0]).distinct().count()
user=RDD_data.map(lambda x:int(x[1])).max()
RDD_user=RDD_data.map(lambda x:(x[1],x[0])).groupByKey().mapValues(list).map(lambda x:[int(x[0]),(x[1],1/len(x[1]))])
start_date=int(RDD_data.map(lambda x:x[3]).min())
RDD_time=RDD_data.map(lambda x:calculateMonths(x))
RDD_join=RDD_user.join(RDD_time)
RDD_final=RDD_join.groupByKey().mapValues(list).map(lambda x:findLastMovie(x)).collect()
data=sc.parallelize([a for i in range(len(RDD_final)) for a in RDD_final[i]])
file_data=data.map(lambda x:toLibfm(x))
file_data.saveAsTextFile('file://~/Project/output')
