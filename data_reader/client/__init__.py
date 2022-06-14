from .filesystem import FSClient
from .s3 import S3Client
from .hdfs import HdfsClient

try:
    from mlc.config import SOURCE
except ImportError:
    SOURCE=None

def get_reader(name=None, **kwargs):
    if name == 's3':
        print('Use S3Client')
        return S3Client(**kwargs)
    if name == 'hdfs':
        print('Use HdfsClient')
        try:
            from mlc.config import HDFS_HOST, HDFS_PORT
            return HdfsClient(HDFS_HOST, HDFS_PORT)
        except:
            pass
    
    print('Use Default Reader')
    return FSClient(**kwargs)