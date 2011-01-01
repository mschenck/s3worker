#!/usr/bin/env python2.6

import hashlib
import re
import simplejson
import sys
import time
from string import Template

from boto.sqs.connection import SQSConnection
from boto.sqs.message import MHMessage
from s3workerlib import log_msg, log_err, log_label, process_config


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

    for job_check_count in xrange(1, min(10, job_count)):
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
    log_label("starting ..." )
    ( tmp_dir, s3_bucket, queue, sleep_time, job_match ) = process_config()
    log_label("start-up complete" )

    # Start job loop
    main_loop()
