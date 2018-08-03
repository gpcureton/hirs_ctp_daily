#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import sys
import traceback
import calendar
import logging
from calendar import monthrange
from time import sleep

from flo.ui import safe_submit_order
from timeutil import TimeInterval, datetime, timedelta

import flo.sw.hirs_ctp_orbital as hirs_ctp_orbital
import flo.sw.hirs_ctp_daily as hirs_ctp_daily
from flo.sw.hirs2nc.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

setup_logging(2)

# General information
hirs2nc_delivery_id = '20180410-1'
hirs_avhrr_delivery_id = '20180505-1'
hirs_csrb_daily_delivery_id  = '20180714-1'
hirs_csrb_monthly_delivery_id  = '20180516-1'
hirs_ctp_orbital_delivery_id  = '20180730-1'
hirs_ctp_daily_delivery_id  = '20180802-1'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)
hour = timedelta(hours=1.)

# Satellite specific information

#satellite = 'noaa-07'
#granule = datetime(2015, 4, 17, 0, 20)
#intervals = [TimeInterval(granule, granule + wedge - wedge)]
# NSS.GHRR.NC.D81236.S0013.E0207.B0086970.WI.level2.hdf --> NSS.GHRR.NC.D85032.S2221.E0009.B1863132.GC.level2.hdf
# datetime(1981, 8, 24, 0, 13) --> datetime(1985, 2, 1, 0, 9)
#intervals = []
#for years in range(1981, 1986):
    #intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]

#satellite = 'noaa-09'
#granule = datetime(2015, 4, 17, 0, 20)
#intervals = [TimeInterval(granule, granule + wedge - wedge)]
# NSS.GHRR.NF.D85056.S0013.E0154.B0105253.GC.level2.hdf --> NSS.GHRR.NF.D88311.S2319.E0107.B2011011.GC.level2.hdf
# datetime(1985, 2, 25, 0, 13) --> datetime(1988, 11, 6, 1, 7)
#intervals = []
#for years in range(1985, 1989):
    #intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]

#satellite = 'noaa-19'
#granule = datetime(2015, 4, 17, 0, 20)
#intervals = [TimeInterval(granule, granule + wedge - wedge)]
# NSS.GHRR.NP.D09108.S2301.E0050.B0100809.GC.level2.hdf --> NSS.GHRR.NP.D17365.S2238.E2359.B4585757.GC.level2.hdf
# datetime(2009, 4, 18, 0, 0) --> datetime(2017, 12, 31, 0, 0)
#intervals = []
#years = 2009
#intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(4,13) ]
#for years in range(2010, 2018):
    #intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]
#years = 2018
#intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,2) ]

satellite = 'metop-b'
#granule = datetime(2015,5,14, 9, 23)
#intervals = [TimeInterval(granule, granule + hour - wedge)]
# NSS.GHRR.M1.D13140.S0029.E0127.B0347172.SV.level2.hdf --> NSS.GHRR.M1.D15365.S2307.E0004.B1705253.SV.level2.hdf
# datetime(2013, 5, 20, 0, 0) --> datetime(2017, 12, 31, 0, 0)
intervals = []
years = 2013
intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(5,13) ]
for years in range(2014,2018):
    intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]

satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

def setup_computation(satellite):

    input_data = {'HIR1B': '/mnt/software/flo/hirs_l1b_datalists/{0:}/HIR1B_{0:}_latest'.format(satellite),
                  'CFSR':  '/mnt/cephfs_data/geoffc/hirs_data_lists/CFSR.out',
                  'PTMSX': '/mnt/software/flo/hirs_l1b_datalists/{0:}/PTMSX_{0:}_latest'.format(satellite)}

    # Data locations
    collection = {'HIR1B': 'ILIAD',
                  'CFSR': 'DELTA',
                  'PTMSX': 'FJORD'}

    input_sources = {'collection':collection, 'input_data':input_data}

    # Initialize the hirs_csrb_daily module with the data locations
    hirs_ctp_daily.set_input_sources(input_sources)

    # Instantiate the computation
    comp = hirs_ctp_daily.HIRS_CTP_DAILY()

    return comp

LOG.info("Submitting intervals...")

dt = datetime.utcnow()
log_name = 'hirs_ctp_daily_{}_s{}_e{}_c{}.log'.format(
    satellite,
    intervals[0].left.strftime('%Y%m%d%H%M'),
    intervals[-1].right.strftime('%Y%m%d%H%M'),
    dt.strftime('%Y%m%d%H%M%S'))

try:

    for interval in intervals:
        LOG.info("Submitting interval {} -> {}".format(interval.left, interval.right))

        comp = setup_computation(satellite)
        hirs_ctp_orbital_comp = hirs_ctp_orbital.HIRS_CTP_ORBITAL()

        contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                                      hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id)

        LOG.info("Opening log file {}".format(log_name))
        file_obj = open(log_name,'a')

        LOG.info("\tThere are {} contexts in this interval".format(len(contexts)))
        contexts.sort()

        if contexts != []:
            #for context in contexts:
                #LOG.info(context)

            LOG.info("\tFirst context: {}".format(contexts[0]))
            LOG.info("\tLast context:  {}".format(contexts[-1]))

            try:
                job_nums = []
                job_nums = safe_submit_order(comp, [comp.dataset('out')], contexts, download_onlies=[hirs_ctp_orbital_comp])

                if job_nums != []:
                    #job_nums = range(len(contexts))
                    #LOG.info("\t{}".format(job_nums))

                    file_obj.write("contexts: [{}, {}]; job numbers: {{{}..{}}}\n".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("contexts: [{}, {}]; job numbers: {{{},{}}}".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("job numbers: {{{}..{}}}\n".format(job_nums[0],job_nums[-1]))
                else:
                    LOG.info("contexts: {{{}, {}}}; --> no jobs".format(contexts[0], contexts[-1]))
                    file_obj.write("contexts: {{{}, {}}}; --> no jobs\n".format(contexts[0], contexts[-1]))
            except Exception:
                LOG.warning(traceback.format_exc())

            #sleep(10.)

        LOG.info("Closing log file {}".format(log_name))
        file_obj.close()

except Exception:
    LOG.warning(traceback.format_exc())
