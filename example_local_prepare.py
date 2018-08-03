#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import sys
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import shutil
import traceback
import logging

from timeutil import TimeInterval, datetime, timedelta
from flo.ui import local_prepare, local_execute

import flo.sw.hirs_ctp_orbital as hirs_ctp_orbital
import flo.sw.hirs_ctp_daily as hirs_ctp_daily

from flo.sw.hirs2nc.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

#
# General information
#

#hirs2nc_delivery_id = '20180410-1'
#hirs_avhrr_delivery_id = '20180505-1'
#hirs_csrb_daily_delivery_id  = '20180714-1'
#hirs_csrb_monthly_delivery_id  = '20180516-1'
#hirs_ctp_orbital_delivery_id  = '20180730-1'
#hirs_ctp_daily_delivery_id  = '20180802-1'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)

# Satellite specific information

#granule = datetime(2017, 1, 1, 0)
#interval = TimeInterval(granule, granule+day-wedge)

def setup_computation(satellite):

    input_data = {'HIR1B': '/mnt/software/flo/hirs_l1b_datalists/{0:}/HIR1B_{0:}_latest'.format(satellite),
                  'CFSR':  '/mnt/cephfs_data/geoffc/hirs_data_lists/CFSR.out',
                  'PTMSX': '/mnt/software/flo/hirs_l1b_datalists/{0:}/PTMSX_{0:}_latest'.format(satellite)}

    # Data locations
    collection = {'HIR1B': 'ILIAD',
                  'CFSR': 'DELTA',
                  'PTMSX': 'FJORD'}

    input_sources = {'collection':collection, 'input_data':input_data}

    # Initialize the hirs_ctp_orbital module with the data locations
    hirs_ctp_daily.set_input_sources(input_sources)

    # Instantiate the computation
    comp = hirs_ctp_daily.HIRS_CTP_DAILY()

    return comp


#
# Local execution
#

def local_execute_example(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                          hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                          hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                          skip_prepare=False, skip_execute=False, single=True, verbosity=2):

    setup_logging(verbosity)

    comp = setup_computation(satellite)
    #comp = hirs_ctp_daily.HIRS_CTP_DAILY()
    hirs_ctp_orbital_comp = hirs_ctp_orbital.HIRS_CTP_ORBITAL()

    # Get the required context...
    contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                                  hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                                  hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id)

    if len(contexts) != 0:
        LOG.info("Candidate contexts in interval...")
        for context in contexts:
            print("\t{}".format(context))

        if not single:

            for idx,context in enumerate(contexts):
                LOG.info('Current Dir: {} {}'.format(idx, os.getcwd()))
                try:
                    if not skip_prepare:
                        LOG.info("Running hirs_ctp_daily local_prepare()...")
                        LOG.info("Preparing context... {}".format(context))
                        local_prepare(comp, context, download_only=[hirs_ctp_orbital_comp])
                    if not skip_execute:
                        LOG.info("Running hirs_ctp_daily local_execute()...")
                        LOG.info("Running context... {}".format(context))
                        local_execute(comp, context)

                    if not skip_prepare:
                        shutil.move('inputs', 'inputs_{}'.format(idx))
                    if not skip_execute:
                        shutil.move('outputs', 'outputs_{}'.format(idx))

                except Exception, err:
                    LOG.error("{}".format(err))
                    LOG.debug(traceback.format_exc())

        else:

            LOG.info("Single context!")
            try:
                if not skip_prepare:
                    LOG.info("Running hirs_ctp_daily local_prepare()...")
                    LOG.info("Preparing context... {}".format(contexts[0]))
                    local_prepare(comp, contexts[0], download_only=[hirs_ctp_orbital_comp])
                if not skip_execute:
                    LOG.info("Running hirs_ctp_daily local_execute()...")
                    LOG.info("Running context... {}".format(contexts[0]))
                    local_execute(comp, contexts[0])
            except Exception, err:
                LOG.error("{}".format(err))
                LOG.debug(traceback.format_exc())
    else:
        LOG.error("There are no valid {} contexts for the interval {}.".format(satellite, interval))

def print_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                   hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                   hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id, verbosity=2):

    setup_logging(verbosity)
    
    comp = setup_computation(satellite)

    contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                                  hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                                  hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id)
    LOG.info('Printing contexts...')
    for context in contexts:
        LOG.info(context)

#satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    #'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    #'noaa-19', 'metop-a', 'metop-b']

#local_execute_example(granule, satellite, hirs_version, collo_version, csrb_version, ctp_version):
