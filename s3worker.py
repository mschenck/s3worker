#!/usr/bin/env python2.6

import re
import sys
import hashlib
import time
import simplejson
import ConfigParser
from string import Template

from boto.sqs.connection import SQSConnection
from boto.sqs.message import MHMessage

from s3workerlib import log_msg, log_err, log_label

config_file = "s3worker.conf"

job_match = {}
tmp_dir = "/tmp/"

def process_job(job):
    log_msg("Picked up job: %s" % job['ID'])
    log_msg("  Job details: %s" % job.get_body())

    if job['STATUS'] == 'READY':
        asset_name = job["ASSET_URL"].split('/')[-1]
        filename = "%s/%s" % (tmp_dir, asset_name)
        basename = "%s/%s" % (tmp_dir, asset_name.split('.')[0])
        log_msg("Picking up asset: %s" % asset_name)

        suffix = "%s" % asset_name.split('.')[-1] 
        log_msg("Suffix: %s" % suffix)

        job_matches = job_match.keys() 
        suffix_check = re.compile( suffix, re.IGNORECASE)
        matched_job = [ job for job in job_matches if re.search(suffix_check, job) ]
        log_msg("Matched %s" % matched_job)

        cmd_pattern = Template(job_match[matched_job[0]])
        cmd = cmd_pattern.substitute(filename=filename, basename=basename) 
        log_msg(cmd)

        #job.delete()


def wait_for_job():
    job_count = 0

    try:
        job_count = queue.count()
        log_msg("Current Job Queue length is %s" % job_count)
    except Exception, e:
        logging.error("Caught exception: %s" % e)

    for job_check_count in xrange(1, job_count):
        job_queue = queue.get_messages(num_messages=job_check_count)
        for job in job_queue:
            if job['STATUS'] == "READY":
                process_job(job)
    else:
        time.sleep(sleep_time)
    return True


def main_loop():
    while wait_for_job():
        pass


if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    log_label("starting ..." )

    try:
        tmp_dir = config.get("global", "tmp_dir")
    except:
        pass
    log_msg( "Temp dir: %s" % tmp_dir)

    try:
        # AWS auth details
        aws_access_key = config.get("AWSauth", "aws_access_key")
        aws_secret_key = config.get("AWSauth", "aws_secret_key")

        # S3 configuration details
        s3_bucket = config.get("S3config", "s3_bucket")

        # SQS job queue configuration details
        queue_name = config.get("SQSconfig", "queue_name")
        conn = SQSConnection(aws_access_key, aws_secret_key)
        queue = conn.create_queue(queue_name)
        queue.set_message_class(MHMessage)

        sleep_time = float(config.get("SQSconfig", "sleep_time"))
        log_msg("sleep time: %s" % sleep_time)

        for section in config.sections():
            if re.search("job", section):
                log_msg("processing section [%s]" % section)
                job_match[config.get(section, "match")] = config.get(section, "exec")

    except Exception, e:
        log_err("Error reading config file [%s]: %s" % (config_file, e))    
        sys.exit(1)

    log_label("start-up complete" )

    # Start job loop
    main_loop()
