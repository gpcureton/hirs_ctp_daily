from flo_deploy.packagelib import *

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__name__)


class HIRS_CTP_DAILY_Package(Package):

    def deploy_package(self):

        for version in ['v20150915']:
            self.merge(Extracted('HIRS_CTP_DAILY_{}.tar.gz'.format(version)).path(), version)
            self.merge(NetcdfFortran().path(), version)
            self.merge(Netcdf().path(), version)
            self.merge(Hdf5().path(), version)
