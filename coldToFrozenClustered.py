import sys
import os
import subprocess
import pipes
import logging

# Change the below settings as appropriate for your environment
ARCHIVE_DIR = '/splunkdata'
REMOTE_SERVER = '192.168.32.128'

# For new style buckets (v4.2+), we can remove all files except for the rawdata.
# We can later rebuild all metadata and tsidx files with "splunk rebuild"


def strip_bucket(base, files):
    """
    Strip all searchable content from buckets, preserve "rawdata" subdir and contents
    :param base: Base path to bucket
    :param files: List of files at the root level of the bucket
    :return:
    """
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full):
            os.remove(full)


def check_remote_path(host, dirpath):
    """
    Check if a path is valid on a remote host
    :param host: Remote server
    :param dirpath: Path to test
    :return:
    """
    try:
        status = subprocess.call(
            ['ssh', host, 'test -d {}'.format(pipes.quote(dirpath))])
        if status == 0:
            return True
        if status == 1:
            return False
    except Exception as ex:
        logging.error(ex)
        raise ex


def create_remote_dir(host, path):
    """
    Create recursive path on remote host
    :param host: Remote server
    :param path: Path to create
    :return:
    """
    try:
        subprocess.call(['ssh', host, 'mkdir -p ', path])
    except Exception as ex:
        logging.error(ex)
        raise ex


def copy_to_frozen(host, source, target):
    """
    Copy cold bucket to frozen path on remote server
    :param host: Target host to copy to
    :param source: Source path to copy from
    :param target: Target path to copy into
    :return:
    """
    cmd = "scp -rq " + source + " " + host + ":" + target
    print cmd
    try:
        os.system(cmd)
    except Exception as ex:
        logging.error(ex)
        raise ex


if __name__ == "__main__":
    del os.environ['LD_LIBRARY_PATH']  # Clear this environmental variable so we don't use splunk's libraries

    if len(sys.argv) != 2:
        sys.exit('usage: python coldToFrozenClustered.py <bucket_dir_to_archive>')

    bucket = sys.argv[1]
    bucketlog = {'bucket': bucket}
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename="/opt/splunk/var/log/splunk/coldtofrozen.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(levelname)s %(bucket)s  %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG)
    logging = logging.LoggerAdapter(logger, bucketlog)
    if not os.path.isdir(bucket):
        logging.error("Bucket is not a valid directory, exiting")
        sys.exit('Bucket is not a valid directory: ' + bucket)
    logging.debug("Starting processing for bucket")
    rawdatadir = os.path.join(bucket, 'rawdata')
    if not os.path.isdir(rawdatadir):
        logging.error("No rawdata directory, bucket invalid, exiting")
        sys.exit('No rawdata directory, given bucket is likely invalid: ' + bucket)

    frozenbucket = os.path.basename(bucket).replace("rb_", "db_")  # get bucket name and swap rb with db
    indexname = os.path.basename(os.path.dirname(os.path.dirname(bucket)))  # get index for building /ARCHIVE/<INDEX>/
    destbucket = os.path.join(ARCHIVE_DIR, indexname, frozenbucket)

    # Test to see if this bucket already exists on the remote server
    if check_remote_path(REMOTE_SERVER, destbucket):
        logging.debug("Bucket already copied to frozen, skipping.")
        exit(0)  # Bucket already exists, ret 0 and let Splunk kill the bucket
    else:
        create_remote_dir(REMOTE_SERVER, destbucket)  # Bucket does not exist, create /ARCHIVE/INDEX/BUCKET
        logging.debug("Bucket has not yet been frozen. New remote_dir=" + destbucket)

    files = os.listdir(bucket)
    journal = os.path.join(rawdatadir, 'journal.gz')
    strip_bucket(bucket, files)  # Delete all files in bucket except rawdata dir contents
    logging.debug("Searchable content successfully stripped")
    copy_to_frozen(REMOTE_SERVER, rawdatadir, destbucket)  # Copy 'rawdata' dir to /ARCHIVE/INDEX/BUCKET
    logging.info("Bucket successfully copied to frozen server.")
    sys.exit(0)
