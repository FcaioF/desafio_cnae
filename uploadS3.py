import boto3
import os

#function used to create bucket in the aws env (if not exists)
def create_bucket(s3_session,bucket_name):

    if s3_session.Bucket(bucket_name) not in s3_session.buckets.all():

        s3_session.create_bucket(Bucket=bucket_name)
        print(f'bucket {bucket_name} criado com sucesso!')

    else:
        print(f'bucket {bucket_name} alredy exist!')

    return

#function used to create directories in s3
def create_directories(bucket_name,directory_name):
    (
     bucket_name
     .put_object(Key=directory_name)   
    )
    return print(f'directory {directory_name} created!')

#function used to upload data to s3 bucket
def upload_data(s3_session,local_path,bucket,s3_path,file_name):
     local_path = local_path + file_name
     s3_path = s3_path + file_name
     s3_session.upload_file(
         local_path
         ,bucket
         ,s3_path)
     return print(f'{file_name} file uploaded!')

#aws profile to boto3 connect
aws_profile_name= 'caio_pessoal_dev'

#bucket name that will be created in s3 
bucket_name= [
    'datalake-bronze-dev-caio'
    ,'datalake-silver-dev-caio'
    ,'datalake-gold-dev-caio'
]

#name of the directories that will be created at s3
directories= [
    'external/flatfile/cnaes/',
    'external/flatfile/estabelecimentos/'
]

#create boto session to aws s3 env
s3_boto_session = boto3.Session(profile_name=aws_profile_name).resource('s3')
print('sucessfuly connected to s3 session!')

#create boto client to aws s3 env
s3_boto_client = boto3.Session(profile_name=aws_profile_name).client('s3')
print('sucessfuly connected to s3 client!')

#bronze bucket object
bronze_bucket= s3_boto_session.Bucket(bucket_name[0])

#create all buckets listed in BUCKET_NAME variable
for i in bucket_name:
     create_bucket(s3_boto_session,i)
#create all directories listed in DIRECTORIES variable
for i in directories:
     create_directories(bronze_bucket,i)

#local machine directories
cnaes_path = 'D://caiof//Documents//GIT_Repos//desafio_cnae//data_source//cnaes//'
estabelecimentos_path = 'D://caiof//Documents//GIT_Repos//desafio_cnae//data_source//estabelecimentos//'

#upload cnaes file
for file in os.listdir(cnaes_path):
    upload_data(s3_boto_client,cnaes_path,bucket_name[0],directories[0],file)

#upload estabelecimentos files
for file in os.listdir(estabelecimentos_path):
    upload_data(s3_boto_client,estabelecimentos_path,bucket_name[0],directories[1],file)

print('all done!')