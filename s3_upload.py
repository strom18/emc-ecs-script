from boto.s3.connection import S3Connection  
import os
import hashlib

accessKeyId = '' # EMC ECS User
secretKey = '' # EMC ECS User secret key
host = 'example.host.lab' # EMC ECS IP Address
port = 9021 # EMC ECS Port => 9020 (no tls) / 9021 for TLS
bucketName = 'example' # Choose your EMC ECS bucket
rootDir = 'c:/example' # Directory to backup to EMC ECS S3

# =================================== function =================================
def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()
# =============================================================================

conn = S3Connection(aws_access_key_id=accessKeyId,  
                    aws_secret_access_key=secretKey,  
                    host=host,  
                    port=port,  
                    calling_format='boto.s3.connection.ProtocolIndependentOrdinaryCallingFormat',  
                    is_secure=True # Port 9020 = False / Port 9021 = True
                    )  

# Connect to bucket
bucket = conn.lookup(bucketName)

# List bucket content
for obj in bucket.get_all_keys():
    print(obj.key)
    

for dirName, subdirList, fileList in os.walk(rootDir):
    root = '%s' % dirName
    for fname in fileList:
        fileName = '%s' % fname
        filePath = root+'/'+fileName
        
        # Check md5 of file before upload
        myFileMd5Checksum = md5Checksum(filePath)
        
        # Upload File
        key = bucket.new_key(filePath)
        key.set_contents_from_filename(filePath)

        # Get S3 etag to check md5
        s3Md5Checksum = bucket.get_key(filePath).etag[1 :-1]

        # Check if MD5 match
        if myFileMd5Checksum == s3Md5Checksum:
            print "[Success] Upload File "+fname+" md5="+s3Md5Checksum
        else:
            print "[Error] Upload File "+fname+" md5="+s3Md5Checksum
