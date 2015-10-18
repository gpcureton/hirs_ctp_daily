from datetime import datetime
from flo.time import TimeInterval
from flo.sw.hirs_ctp_orbital import HIRS_CTP_ORBITAL
from flo.sw.hirs_ctp_daily import HIRS_CTP_DAILY
from flo.ui import submit_order
import logging
import sys
import time

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)


def submit(logger, interval, platform):

    hirs_version = 'v20151014'
    collo_version = 'v20140204'
    csrb_version = 'v20140204'
    ctp_version = 'v20150915'

    c = HIRS_CTP_DAILY()
    contexts = c.find_contexts(platform, hirs_version, collo_version, csrb_version, ctp_version,
                               interval)

    while 1:
        try:
            return submit_order(c, [c.dataset('out')], contexts, (HIRS_CTP_ORBITAL(),))
        except:
            time.sleep(5*60)
            logger.info('Failed submiting jobs.  Sleeping for 5 minutes and submitting again')

# Setup Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

# Submitting Jobs
for platform in ['metop-a']:
    for interval in [TimeInterval(datetime(2009, 1, 1), datetime(2009, 2, 1))]:
        jobIDRange = submit(logger, interval, platform)

        if len(jobIDRange) > 0:
            logger.info('Submitting hirs_ctp_daily jobs for {} '.format(platform) +
                        'from {} to {}'.format(interval.left, interval.right))
        else:
            logger.info('No hirs_ctp_daily jobs for {} '.format(platform) +
                        'from {} to {}'.format(interval.left, interval.right))
