#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
import logging
import traceback
from subprocess import CalledProcessError

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs_ctp_orbital as hirs_ctp_orbital
from flo.sw.hirs2nc.delta import DeltaCatalog
from flo.sw.hirs2nc.utils import link_files

# every module should have a LOG object
LOG = logging.getLogger(__name__)

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_CTP_DAILY(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id',
                  'hirs_csrb_daily_delivery_id', 'hirs_csrb_monthly_delivery_id',
                  'hirs_ctp_orbital_delivery_id', 'hirs_ctp_daily_delivery_id']
    outputs = ['out']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                      hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        LOG.debug("Running find_contexts()")
        return [{'granule': g,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id,
                 'hirs_csrb_monthly_delivery_id': hirs_csrb_monthly_delivery_id,
                 'hirs_ctp_orbital_delivery_id': hirs_ctp_orbital_delivery_id,
                 'hirs_ctp_daily_delivery_id': hirs_ctp_daily_delivery_id}
                for g in granules]

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_DAILY')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")

        # Initialize the hirs_ctp_orbital module with the data locations
        hirs_ctp_orbital.delta_catalog = delta_catalog

        # Instantiate the hirs_ctp_orbital computation
        hirs_ctp_orbital_comp = hirs_ctp_orbital.HIRS_CTP_ORBITAL()

        SPC = StoredProductCatalog()

        # CTP Orbital Input

        day = TimeInterval(context['granule'], (context['granule'] + timedelta(days=1) -
                                                timedelta(seconds=1)))
        LOG.debug("day: {}".format(day))

        hirs_ctp_orbital_contexts = hirs_ctp_orbital_comp.find_contexts(
                                                                day,
                                                                context['satellite'],
                                                                context['hirs2nc_delivery_id'],
                                                                context['hirs_avhrr_delivery_id'],
                                                                context['hirs_csrb_daily_delivery_id'],
                                                                context['hirs_csrb_monthly_delivery_id'],
                                                                context['hirs_ctp_orbital_delivery_id'])

        if len(hirs_ctp_orbital_contexts) == 0:
            raise WorkflowNotReady('No HIRS_CTP_ORBITAL inputs available for {}'.format(context['granule']))

        for idx,context in enumerate(hirs_ctp_orbital_contexts):
            hirs_ctp_orbital_prod = hirs_ctp_orbital_comp.dataset('out').product(context)
            if SPC.exists(hirs_ctp_orbital_prod):
                task.input('CTPO-{}'.format(idx), hirs_ctp_orbital_prod)

    def create_ctp_daily(self, inputs, context):
        '''
        Create the CFSR statistics for the current day.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CTP script locations
        hirs_ctp_daily_delivery_id = context['hirs_ctp_daily_delivery_id']
        delivery = delivered_software.lookup('hirs_ctp_daily', delivery_id=hirs_ctp_daily_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        version = delivery.version

        # Determine the output filenames
        output_file = 'hirs_ctp_daily_{}_{}.nc'.format(context['satellite'],
                                                       context['granule'].strftime('D%y%j'))
        LOG.info("output_file: {}".format(output_file))

        # Generating CTP Orbital Input List
        ctp_orbital_file = 'ctp_orbital_list'
        with open(ctp_orbital_file, 'w') as f:
            [f.write('{}\n'.format(basename(input))) for input in inputs.values()]

        # Run the CTP daily binary
        ctp_daily_bin = pjoin(dist_root, 'bin/create_daily_daynight_ctps.exe')
        cmd = '{} {} {}'.format(
                ctp_daily_bin,
                ctp_orbital_file,
                output_file
                )
        #cmd = 'sleep 0.5; touch {}'.format(output_file)

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_ctp = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_ctp = err.returncode
            LOG.error(" CTP daily binary {} returned a value of {}".format(ctp_daily_bin, rc_ctp))
            return rc_ctp, None

        # Verify output file
        output_file = glob(output_file)
        if len(output_file) != 0:
            output_file = output_file[0]
            LOG.debug('Found output CTP daily file "{}"'.format(output_file))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_file))
            rc = 1
            return rc, None

        return rc, output_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_DAILY')
    def run_task(self, inputs, context):
        '''
        Run the CTP Daily binary on a single context
        '''

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Link the inputs into the working directory
        inputs = symlink_inputs_to_working_dir(inputs)

        # Create the CTP daily file for the current day.
        rc, ctp_daily_file = self.create_ctp_daily(inputs, context)

        return {'out': nc_compress(ctp_daily_file)}
