import unittest
from Properties import PLSEnvironments
from operations import PerformanceHelpers
from operations.PerformanceHelpers import PerformanceTestRunner
from operations.TestHelpers import DLConfigRunner, JamsRunner

__author__ = 'WBu'


class Test(unittest.TestCase):
    def Template20_InsightsAllSteps_1ELQThreads(self):
        loadGroups = ["InsightsAllSteps"]
        tenants = []

        dlConfig = DLConfigRunner()
        jamsRunner = JamsRunner()
        # Tenants 1-5 are updated to LP template 2.1
        for i in range(1, 2):
            tenant = "BI_Perf_10_27_ELQ%s" % i
            tenants.append(tenant)
            # update Dante Data providers and config Jams for these tenants
            dlConfig.editDataProviders(tenant, "SQL_DanteDB_DataProvider", PLSEnvironments.SQL_DanteDB_DataProvider);
            jamsRunner.setJamsTenant(tenant)
        # Concurrently start 5 threads to run load group
        for t in tenants:
            test = PerformanceTestRunner(t, loadGroups)
            # time.sleep(10)
            test.start()
        PerformanceHelpers.perf_waitForAllThreadFinished()

    def Template21_InsightsAllSteps_1ELQThreads(self):
        loadGroups = ["InsightsAllSteps"]
        tenants = []
        dlConfig = DLConfigRunner()
        jamsRunner = JamsRunner()
        # Tenants 6-10 are updated to LP template 2.1
        for i in range(6, 7):
            tenant = "BI_Perf_10_27_ELQ%s" % i
            tenants.append(tenant)
            # update Dante Data providers and config Jams for these tenants
            dlConfig.editDataProviders(tenant, "SQL_DanteDB_DataProvider", PLSEnvironments.SQL_DanteDB_DataProvider);
            jamsRunner.setJamsTenant(tenant)

        # Concurrently start 5 threads to run load group
        for t in tenants:
            test = PerformanceTestRunner(t, loadGroups)
            # time.sleep(10)
            test.start()
        PerformanceHelpers.perf_waitForAllThreadFinished()

    def Template20_InsightsAllSteps_5ELQThreads(self):
        loadGroups = ["InsightsAllSteps"]
        tenants = []

        dlConfig = DLConfigRunner()
        jamsRunner = JamsRunner()
        # Tenants 1-5 are updated to LP template 2.1
        for i in range(1, 6):
            tenant = "BI_Perf_10_27_ELQ%s" % i
            tenants.append(tenant)
            # update Dante Data providers and config Jams for these tenants
            dlConfig.editDataProviders(tenant, "SQL_DanteDB_DataProvider", PLSEnvironments.SQL_DanteDB_DataProvider);
            jamsRunner.setJamsTenant(tenant)
        # Concurrently start 5 threads to run load group
        for t in tenants:
            test = PerformanceTestRunner(t, loadGroups)
            # time.sleep(10)
            test.start()
        PerformanceHelpers.perf_waitForAllThreadFinished()

    def Template21_InsightsAllSteps_5ELQThreads(self):
        loadGroups = ["InsightsAllSteps"]
        tenants = []
        dlConfig = DLConfigRunner()
        jamsRunner = JamsRunner()
        # Tenants 6-10 are updated to LP template 2.1
        for i in range(6, 11):
            tenant = "BI_Perf_10_27_ELQ%s" % i
            tenants.append(tenant)
            # update Dante Data providers and config Jams for these tenants
            dlConfig.editDataProviders(tenant, "SQL_DanteDB_DataProvider", PLSEnvironments.SQL_DanteDB_DataProvider);
            jamsRunner.setJamsTenant(tenant)

        # Concurrently start 5 threads to run load group
        for t in tenants:
            test = PerformanceTestRunner(t, loadGroups)
            # time.sleep(10)
            test.start()
        PerformanceHelpers.perf_waitForAllThreadFinished()


if __name__ == "__main__":
    unittest.main()
