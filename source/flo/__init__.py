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

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    #delivered_software,
    #support_software,
    #runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs_ctp_orbital as hirs_ctp_orbital
from flo.sw.hirs.delta import DeltaCatalog

# every module should have a LOG object
LOG = logging.getLogger(__name__)

SPC = StoredProductCatalog()

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_CTP_DAILY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version']
    outputs = ['out']

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_DAILY')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")
        LOG.debug("context:  {}".format(context))

        # Initialize the hirs and hirs_avhrr modules with the data locations
        hirs_ctp_orbital.delta_catalog = delta_catalog

        # Instantiate the hirs_ctp_orbital computation
        hirs_ctp_orbital_comp = hirs_ctp_orbital.HIRS_CTP_ORBITAL()

        day = TimeInterval(context['granule'], (context['granule'] + timedelta(days=1) -
                                                timedelta(seconds=1)))

        hirs_ctp_orbital_contexts = hirs_ctp_orbital_comp.find_contexts(
                                                                day,
                                                                context['sat'],
                                                                context['hirs_version'],
                                                                context['collo_version'],
                                                                context['csrb_version'],
                                                                context['ctp_version'])

        if len(hirs_ctp_orbital_contexts) == 0:
            raise WorkflowNotReady('No HIRS_CTP_ORBITAL inputs available for {}'.format(context['granule']))

        for (i, c) in enumerate(hirs_ctp_orbital_contexts):
            hirs_ctp_orbital_prod = hirs_ctp_orbital_comp.dataset('out').product(c)
            if SPC.exists(hirs_ctp_orbital_prod):
                task.input('CTPO-{}'.format(i), hirs_ctp_orbital_prod, True)

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_DAILY')
    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['ctp_version'], 'lib')

        output = 'ctp.daily.{}.{}.nc'.format(context['sat'], context['granule'].strftime('D%y%j'))

        # Generating CTP Orbital Input Lis
        ctp_orbital_file = 'ctp_orbital_list'
        with open(ctp_orbital_file, 'w') as f:
            [f.write('{}\n'.format(input)) for input in inputs.values()]

        cmd = os.path.join(self.package_root, context['ctp_version'],
                           'bin/create_daily_daynight_ctps.exe')
        cmd += ' {} {}'.format(ctp_orbital_file, output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, time_interval, sat, hirs_version, collo_version, csrb_version, ctp_version):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        return [{'granule': g,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CTP_DAILY')
