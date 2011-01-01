#!/usr/bin/env python2.6

import sys
import hashlib
import logging
import logging.handlers
import time
import simplejson
import ConfigParser

from boto.sqs.connection import SQSConnection
from boto.sqs.message import MHMessage

config_file = "s3worker.conf"

def process_job(job):
    logging.info("Picked up job: %s" % job['ID'])
    logging.info("  Job details: %s" % job.get_body())
    #job.delete()


def wait_for_job():
    job_count = 0

    try:
        job_count = queue.count()
        logging.info("Current Job Queue length is %s" % job_count)
    except Exception, e:
        logging.error("Caught exception: %s" % e)

    for job_check_count in xrange(1, job_count):
        job_queue = queue.get_messages(num_messages=job_check_count)
        for job in job_queue:
            if job['STATUS'] == "READY":
                process_job(job)
        time.sleep(sleep_time)

    else:
        time.sleep(sleep_time)
    return True


def main_loop():
    while wait_for_job():
        pass


if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    logging.getLogger().setLevel(logging.INFO)

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

    except Exception, e:
        logging.error("Error reading config file [%s]: %s" % (config_file, e))    
        sys.exit(1)

    # Start job loop
    main_loop()
