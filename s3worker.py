#!/usr/bin/env python2.6

import hashlib
import os
import re
import simplejson
import sys
import time
import urllib2
from string import Template

from boto.sqs.connection import SQSConnection
from boto.sqs.message import MHMessage
from s3workerlib import log_msg, log_err, log_debug, log_label, process_config


def process_job(job):
    log_msg("Picked up job: %s" % job['ID'])
    log_debug("details:%s" % job.get_body())
    template_mappings = {}

    try:
        # Pull down the asset and assign template variables
        asset_name = job["ASSET_URL"].split('/')[-1]

        filename = "%s/%s" % (tmp_dir, asset_name)
        basename = "%s/%s" % (tmp_dir, asset_name.split('.')[0])
        template_mappings["filename"] = filename
        template_mappings["basename"] = basename

        # "wget" asset_url to $filename
        asset = open(filename, "wb")
        asset.write( wget.open(job["ASSET_URL"]).read() )
        asset.close()
    
        # Take ownership for job
        job.update( { "STATUS": "PROCESSING" } )
        log_msg("Picked up asset: %s" % asset_name)
    except Exception, e:
        log_err("Caught exception taking job: %s" % e)
 
    try: 
        # Attempt to match suffix with job sections
        asset_suffix = "%s" % asset_name.split('.')[-1] 
        log_debug("Suffix: %s" % asset_suffix)
        suffixes = job_processes.keys() 
        suffix_check = re.compile( asset_suffix, re.IGNORECASE)
        matched_suffix = [ suffix for suffix in suffixes if re.search(suffix_check, suffix) ]
        if matched_suffix:
            log_debug("Matched suffix '%s'" % matched_suffix)
        else:
            log_err("Could not match any job configs to files with suffix '%s'" % suffix)
    except Exception, e:
        log_err("Caught exception matching execs to suffix" % e)
 
    try: 
        # Perform commands on asset
        exec_list = job_processes[matched_suffix[0]]
        for command_template in exec_list:
            log_debug("Command template [ %s ]" % command_template)

            cmd_pattern = Template(command_template)
            cmd = cmd_pattern.substitute(template_mappings) 

            log_msg("Running command [%s]" % cmd)
            os.system(cmd)
 
        # If successful, delete job
        queue.delete_message(job)
    except Exception, e:
        log_err("Caught exception processing job: %s" % e)


def wait_for_job():
    job_count = 0

    try:
        job_count = queue.count()
        log_msg("Current Job Queue length is %s" % job_count)
    except Exception, e:
        logging.err("Caught exception: %s" % e)

    if job_count > 0:
        try:
            job = queue.get_messages()[0]
            if job['STATUS'] == "READY":
                process_job(job)
        except Exception, e:
            log_err("Exception attempting to read from queue with job count %s: %s" % (job_count, e))

    return True


def main_loop():
    while wait_for_job():
        time.sleep(sleep_time)


if __name__ == "__main__":
    log_label("starting ..." )
    ( tmp_dir, s3_bucket, queue, sleep_time, job_processes ) = process_config()
    wget = urllib2.build_opener()
    log_label("start-up complete" )

    # Start job loop
    main_loop()
