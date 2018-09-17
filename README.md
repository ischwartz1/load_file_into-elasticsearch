# load_file_into-elasticsearch
python
import	boto3 
import gzip
import sys
#download file

s3=boto3.client('s3')
s3.download_file('perion-test-candidates', 'source_data/elastic_data.log.gz', 'local_elastic_data.log.gz')

from elasticsearch import Elasticsearch
es = Elasticsearch(HOST="localhost:9200")

file = open("failed_lines.log","a") 
xx=0
yy=0
zz=0
with gzip.open('local_elastic_data.log.gz', 'rb') as f:
	for line in f:
		try:
			xx=xx+1      
         		es.index(index="isa",doc_type="logs",id=xx,body=line)
			zz=zz+1
		except:
			file.write(line)
			yy=yy+1



	 
file.close()

temp = sys.stdout 
sys.stdout = open('log.txt', 'w')
print("total_lines: "+str(xx)+ " , failed: "+str(yy)+", success:"+str(zz))
sys.stdout.close()
sys.stdout = temp 

#upload file

s3.upload_file('failed_lines.log','perion-test-candidates','isa/failed_lines.log')


##total_lines: 1000000 , failed: 251, success:999749

