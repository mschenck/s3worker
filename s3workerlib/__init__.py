import ConfigParser
import logging
import logging.handlers
import re
import sys
import time

from boto.sqs.connection import SQSConnection
from boto.sqs.message import MHMessage

config_file = "s3worker.conf"
tmp_dir = "/tmp"
log_file = "/tmp/s3worker.log"
job_match = {}

################################################################################
# Logging details
################################################################################
s3logger = logging.getLogger('s3worker')
LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


def log_label(msg):
    s3logger.info("#" * 80)
    s3logger.info("# %s" % msg)
    s3logger.info("#" * 80)


def log_msg(msg):
    s3logger.info("s3worker:%s: %s" % (time.strftime("%c"), msg))


def log_err(msg):
    s3logger.error("s3worker:%s: %s" % (time.strftime("%c"), msg))


def log_debug(msg):
    s3logger.debug("s3worker:%s: %s" % (time.strftime("%c"), msg))


################################################################################
# Configuration processing details
################################################################################
def process_config():
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    try:
        tmp_dir = config.get("global", "tmp_dir")
        log_file = config.get("global", "log_file")
        log_level = config.get("global", "log_level")
    except:
        pass

    s3logger.setLevel(LEVELS.get(log_level, logging.INFO))
    file_handler = logging.handlers.WatchedFileHandler(log_file)
    s3logger.addHandler(file_handler)

    log_label("starting ..." )
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
                suffix = config.get(section, "match")
                log_msg("processing section [%s]" % section)
               
                job_match[suffix] = []
                for option in config.options(section):
                    if re.search("exec", option):
                        job_match[suffix].append( config.get(section, option) )
                       
                log_debug("CMDs for suffix [png]") 
                for cmd in job_match[suffix]:
                    log_debug("   %s" % cmd)

    except Exception, e:
        log_err("Error reading config file [%s]: %s" % (config_file, e))
        sys.exit(1)

    return ( tmp_dir, s3_bucket, queue, sleep_time, job_match )
