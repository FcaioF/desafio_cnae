import boto3

#function used to create bucket in the aws env (if not exists)
def createBucket(s3_session,bucket_name):

    if s3_session.Bucket(bucket_name) not in s3_session.buckets.all():

        s3_session.create_bucket(Bucket=bucket_name)
        print(f'bucket {bucket_name} criado com sucesso!')

    else:
        print(f'bucket {bucket_name} alredy exist!')

    return
    

def upload_data(bucket_name,directory_name):
    (
     bucket_name
     .put_object(Key=directory_name)   
    )
    return print('directory created!')

#aws profile to boto3 connect
aws_profile_name= 'caio_pessoal_dev'

#bucket name that will be created in s3 
bucket_name= [
    'datalake-bronze-dev-caio'
    ,'datalake-silver-dev-caio'
    ,'datalake-gold-dev-caio'
]

#directories to be created
s3_directories= [

]

#creatin boto session to aws env
s3_boto_session = boto3.Session(profile_name=aws_profile_name).resource('s3')
print('sucessfuly connected to s3!')


for i in bucket_name:
     createBucket(s3_boto_session,i)



# print(s3_boto_session.Bucket('datalake-bronze-dev-caio'))


# for a in s3.buckets.all():
#     print(a)





