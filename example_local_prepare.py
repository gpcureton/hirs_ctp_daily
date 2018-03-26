#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import sys
import traceback
import logging

from timeutil import TimeInterval, datetime, timedelta
from flo.ui import local_prepare, local_execute

import flo.sw.hirs_ctp_orbital as hirs_ctp_orbital
import flo.sw.hirs_ctp_daily as hirs_ctp_daily
from flo.sw.hirs.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

#
# Local execution
#

# General information
satellite = 'metop-b'
hirs_version  = 'v20151014'
collo_version = 'v20151014'
csrb_version  = 'v20150915'
ctp_version = 'v20150915'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)

# Satellite specific information

#granule = datetime(2017, 1, 1, 0)
#interval = TimeInterval(granule, granule+day-wedge)

# Data locations
collection = {'HIR1B': 'ILIAD',
              'CFSR': 'DELTA',
              'PTMSX': 'ILIAD'}
# NOAA-19
#satellite = 'noaa-19'
#input_data = {'HIR1B': '/mnt/sdata/geoffc/HIRS_processing/data_lists/NOAA-19/HIR1B_noaa-19_latest',
              #'CFSR':  '/mnt/sdata/geoffc/HIRS_processing/data_lists/CFSR.out',
              #'PTMSX': '/mnt/sdata/geoffc/HIRS_processing/data_lists/NOAA-19/PTMSX_noaa-19_latest'}

# Metop-B
#satellite = 'metop-b'
input_data = {'HIR1B': '/mnt/cephfs_data/geoffc/hirs_data_lists/Metop-B/HIR1B_metop-b_latest',
              'CFSR':  '/mnt/cephfs_data/geoffc/hirs_data_lists/CFSR.out',
              'PTMSX': '/mnt/cephfs_data/geoffc/hirs_data_lists/Metop-B/PTMSX_metop-b_latest'}

input_sources = {'collection':collection, 'input_data':input_data}

# Initialize the hirs_avhrr module with the data locations
hirs_ctp_daily.set_input_sources(input_sources)

# Instantiate the computations
hirs_ctp_orbital_comp = hirs_ctp_orbital.HIRS_CTP_ORBITAL()
comp = hirs_ctp_daily.HIRS_CTP_DAILY()

# General information

#
# Local execution
#

def local_execute_example(interval, satellite, hirs_version, collo_version, csrb_version, ctp_version,
                          skip_prepare=False, skip_execute=False, verbosity=2):

    setup_logging(verbosity)

    # Get the required context...
    LOG.info("Getting required contexts for local execution...")
    contexts =  comp.find_contexts(interval, satellite, hirs_version, collo_version, csrb_version, ctp_version)
    LOG.info("Finished getting required contexts for local execution\n")

    if len(contexts) != 0:
        LOG.info("Candidate contexts in interval...")
        for context in contexts:
            print("\t{}".format(context))

        try:
            if not skip_prepare:
                LOG.info("Running hirs_csrb_daily local_prepare()...")
                LOG.info("Preparing context... {}".format(contexts[0]))
                local_prepare(comp, contexts[0], download_only=[hirs_ctp_orbital_comp])
            if not skip_execute:
                LOG.info("Running hirs_csrb_daily local_execute()...")
                LOG.info("Running context... {}".format(contexts[0]))
                local_execute(comp, contexts[0])
        except Exception, err:
            LOG.error("{}".format(err))
            LOG.debug(traceback.format_exc())
    else:
        LOG.error("There are no valid {} contexts for the interval {}.".format(satellite, interval))


def print_contexts(interval, satellite, hirs_version, collo_version, csrb_version, ctp_version, verbosity=2):
    setup_logging(verbosity)
    contexts = comp.find_contexts(interval, satellite, hirs_version, collo_version, csrb_version, ctp_version)
    for context in contexts:
        LOG.info(context)

#satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    #'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    #'noaa-19', 'metop-a', 'metop-b']

#local_execute_example(granule, satellite, hirs_version, collo_version, csrb_version, ctp_version):
