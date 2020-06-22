#!/usr/bin/env python

# Execution: nohup python restoreFrozen.py &

######################################################
## start of variables ##
######################################################

epoch_oldest = "1546347599"  ## start time (in epoch seconds) of events to be thawed ##
epoch_newest = "1563148800"  ## end time (in epoch seconds) of events to be thawed ##
frozen_dir = "/d/d3/web_technology/frozendb/"  ## base directory for frozen data - don't forget trailing "/" ##
thawed_dir = "/d/d3/thawed/web_technology/thaweddb/"  ## base directory for thawed data - don't forget trailing "/" ##
log_file = "/tmp/restoreLog.txt"  ## log file for bucket rebuilds and errors ##
splunk_binary = "/opt/splunk/bin/splunk"  ## splunk binary used for the rebuild command ##
chown_command = "/usr/bin/chown"  ## chown binary used to set permissions ##

######################################################
## end of variables ##
######################################################

import os
import time
import shutil
import subprocess

bucket_count = 0
bucket_iterator = 0
# count number of eligible buckets:
for frozen_buckets in sorted(os.listdir(frozen_dir)):

    try:
        bucket_type = frozen_buckets.split("_")[0]
        bucket_epoch_newest = frozen_buckets.split("_")[1]
        bucket_epoch_oldest = frozen_buckets.split("_")[2]
        if bucket_type == "db":
            if (int(bucket_epoch_newest) >= int(epoch_oldest) and int(bucket_epoch_newest) <= int(epoch_newest)) or (
                    int(bucket_epoch_oldest) >= int(epoch_oldest) and int(bucket_epoch_oldest) <= int(epoch_newest)):
                bucket_count = bucket_count + 1
    except BaseException as error:
        file = open(log_file, 'a')
        file.write(time.strftime('%x %X') + " --- " + str(error) + "\n")
        file.close
        continue

file = open(log_file, 'a')
file.write(time.strftime('%x %X') + " --- " + "Starting thawing " + str(bucket_count) + " eligible buckets" + "\n")
file.close

# iterate over buckets
for frozen_buckets in sorted(os.listdir(frozen_dir)):

    try:
        bucket_type = frozen_buckets.split("_")[0]
        bucket_epoch_newest = frozen_buckets.split("_")[1]
        bucket_epoch_oldest = frozen_buckets.split("_")[2]
        if bucket_type == "db":
            if (int(bucket_epoch_newest) >= int(epoch_oldest) and int(bucket_epoch_newest) <= int(epoch_newest)) or (
                    int(bucket_epoch_oldest) >= int(epoch_oldest) and int(bucket_epoch_oldest) <= int(epoch_newest)):
                bucket_iterator = bucket_iterator + 1
                shutil.copytree(frozen_dir + frozen_buckets, thawed_dir + frozen_buckets)
                subprocess.call([chown_command, "-R", "splunk:splunk", thawed_dir + frozen_buckets])
                subprocess.call([splunk_binary, "rebuild", thawed_dir + frozen_buckets])
                with open(log_file, 'a') as file:
                    file.write(time.strftime('%x %X') + " -- " + "Bucket#: " + str(bucket_iterator) + " of " + str(
                        bucket_count) + " --- " + thawed_dir + frozen_buckets + " thawed and rebuilt\n")
    except BaseException as error:
        file = open(log_file, 'a')
        file.write(time.strftime('%x %X') + " --- " + "Bucket#: " + str(bucket_iterator) + " of " + str(
            bucket_count) + " --- " + str(error) + "\n")
        file.close
        continue