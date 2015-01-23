from flo_deploy.packagelib import *


class HIRS_CTP_DAILY_Package(Package):

    def deploy_package(self):

        for version in ['v20140204']:
            self.merge(Extracted('HIRS_CTP_DAILY_v20140204.tar.gz'.format(version)).path(), version)
            self.merge(NetcdfFortran().path(), version)
            self.merge(Netcdf().path(), version)
            self.merge(Hdf5().path(), version)
