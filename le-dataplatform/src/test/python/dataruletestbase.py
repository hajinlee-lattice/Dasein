import fastavro as avro
import glob
import os
import shutil
import sys

from trainingtestbase import TrainingTestBase

class DataRuleTestBase(TrainingTestBase):

    def setUp(self):
        super(DataRuleTestBase, self).setUp()

    def tearDown(self):
        super(DataRuleTestBase, self).tearDown()

    def assertRuleOutputCount(self, expectedCount):
        self.assertEqual(len(glob.glob("./results/datarules/*.avro")), expectedCount)

    def getDataDirectory(self):
        cwd = os.getcwd()
        if os.path.exists("./data/"):
            return cwd + "/data/"
        elif os.path.exists("../data/"):
            return cwd + "/../data"

    def assertColumnRuleOutput(self, ruleOutput, expectedColumns):
        actualColumns = []
        if os.path.isfile(ruleOutput):
            with open(ruleOutput) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    actualColumns.append(record["itemid"])

        self.assertSetEqual(set(expectedColumns), set(actualColumns), "")

    def assertRowRuleOutput(self, ruleOutput, expectedDict):
        self.assertTrue(os.path.exists(ruleOutput))

        expectedRows = {}
        for key, columns in expectedDict.iteritems():
            expectedRows[key] = ','.join(columns)

        actualRows = {}
        if os.path.isfile(ruleOutput):
            with open(ruleOutput) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    actualRows[record["itemid"]] = record["columns"]

        self.assertDictEqual(expectedRows, actualRows, "")

    def assertStandardRuleOutputs(self):
        self.assertColumnRuleOutput("./results/datarules/CountUniqueValueRule_ColumnRule.avro", [])
        self.assertColumnRuleOutput("./results/datarules/PopulatedRowCount_ColumnRule.avro", [])
        self.assertColumnRuleOutput("./results/datarules/FrequencyIssue_ColumnRule.avro", ['Activity_ClickEmail_cnt', 'BW_ads', 'CloudTechnologies_CRM_Two', 'BW_TotalTech_Cnt', 'CloudTechnologies_ProjectMgnt_Two', 'BusinessVCFunded', 'BW_shop', 'Activity_OpenEmail_cnt', 'ModelAction', 'BW_ns', 'BusinessFirmographicsParentRevenue', 'CloudTechnologies_HardwareBasic_One', 'BusinessSocialPresence', 'BW_Web_Server', 'CloudTechnologies_EnterpriseApplications_One', 'BW_hosting', 'BW_analytics', 'DerogatoryIndicator', 'UCCFilingsPresent', 'BusinessEstimatedEmployees', 'BW_mapping', 'BusinessEstimatedAnnualSales_k', 'PercentileModel', 'ExperianCreditRating', 'CloudTechnologies_SoftwareBasic_Two', 'BusinessEstablishedYear', 'CloudTechnologies_CloudService_Two', 'BW_javascript', 'CloudTechnologies_ProductivitySltns_One', 'BW_framework', 'CloudTechnologies_MarketingPerfMgmt_One', 'FundingAgency', 'CloudTechnologies_EnterpriseContent_Two', 'BW_Server', 'Activity_VisitWeb_cnt', 'CloudTechnologies_CloudService_One', 'Alexa_Rank', 'BusinessECommerceSite', 'PD_DA_AwardCategory', 'Activity_ClickLink_cnt', 'Years_in_Business_Code', 'BW_cdn', 'BW_encoding', 'CloudTechnologies_CommTech_Two', 'Non_Profit_Indicator', 'CloudTechnologies_ProductivitySltns_Two', 'UCCFilings', 'Alexa_ViewsPerUser', 'BusinessUrlNumPages', 'CloudTechnologies_SoftwareBasic_One', 'BW_parked', 'FundingStage', 'PD_DA_MonthsPatentGranted', 'BW_cms', 'BW_payment', 'PD_DA_JobTitle', 'JobsTrendString', 'BusinessRetirementParticipants', 'CloudTechnologies_CRM_One', 'BW_cdns', 'JobsRecentJobs', 'BW_docinfo', 'BW_seo_headers', 'CloudTechnologies_CommTech_One', 'CloudTechnologies_EnterpriseApplications_Two', 'BW_ssl', 'PD_DA_LastSocialActivity_Units', 'Activity_UnsubscrbEmail_cnt', 'CloudTechnologies_ITGovernance_One', 'Alexa_MonthsSinceOnline', 'CloudTechnologies_MarketingPerfMgmt_Two', 'BW_mx', 'CloudTechnologies_EnterpriseContent_One', 'Alexa_ReachPerMillion', 'BW_TechTags_Cnt', 'CloudTechnologies_HardwareBasic_Two', 'BW_feeds', 'CloudTechnologies_ProjectMgnt_One', 'Intelliscore', 'Alexa_ViewsPerMillion', 'BW_Web_Master', 'PD_DA_PrimarySIC1', 'BusinessAnnualSalesAbs', 'BW_seo_title', 'BW_media', 'BW_seo_meta', 'CloudTechnologies_ITGovernance_Two', 'BankruptcyFiled', 'BW_widgets', 'Activity_FillOutForm_cnt', 'Activity_InterestingMoment_cnt', 'Activity_EmailBncedSft_cnt', 'BusinessFirmographicsParentEmployees'])
        self.assertColumnRuleOutput("./results/datarules/FutureInformation_ColumnRule.avro", ['FundingAgency'])
        self.assertColumnRuleOutput("./results/datarules/HighlyPredictiveSmallPopulation_ColumnRule.avro", ['ExperianCreditRating', 'FundingAgency', 'Years_in_Business_Code', 'PD_DA_JobTitle', 'PD_DA_LastSocialActivity_Units'])
        self.assertColumnRuleOutput("./results/datarules/NullIssue_ColumnRule.avro", ['FundingAmount', 'FundingAgency', 'FundingFinanceRound', 'PD_DA_AwardCategory', 'Activity_ClickLink_cnt', 'Years_in_Business_Code', 'BW_encoding', 'FundingStage', 'BW_cdns', 'BW_seo_headers', 'FundingReceived', 'Activity_UnsubscrbEmail_cnt', 'FundingAwardAmount', 'BW_seo_title', 'BW_seo_meta', 'BankruptcyFiled', 'PD_DA_MonthsSinceFundAwardDate'])

