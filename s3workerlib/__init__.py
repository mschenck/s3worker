import time
import logging
import logging.handlers

logging.getLogger().setLevel(logging.INFO)

def log_label(msg):
    logging.info("#" * 80)
    logging.info("# %s" % msg)
    logging.info("#" * 80)

def log_msg(msg):
    logging.info("s3worker:%s: %s" % (time.strftime("%c"), msg))
