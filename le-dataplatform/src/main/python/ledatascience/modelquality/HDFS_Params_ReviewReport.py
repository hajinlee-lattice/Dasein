import os
class hdfsRevParam(object):

    def __init__(self, modelName, modelDirectoryPath, trainingFilePath, configParametersPath):
        self.ModelName = modelName
        self.modelDirectoryPath = modelDirectoryPath
        self.topPredictor_file =  self.modelDirectoryPath + "/" + self.ModelName + "_model.csv"
        self.outputHDFSDirectory = modelDirectoryPath + "/enhancements/analytics"
        self.configParametersPath = configParametersPath
        self.trainingFilePath = trainingFilePath

        # Dont have to change, file names are same for all models
        self.modelsummaryJson_file = self.modelDirectoryPath + "/enhancements/modelsummary.json"
        self.rfmodel_file = self.modelDirectoryPath +  "/rf_model.txt"

        self.output_file = self.outputHDFSDirectory + "/Flags.xlsx"

        # Event tablefiles
        #self.trainDataFile = self.trainingFilePath + "/Training.csv"

        self.trainDataFile = self.trainingFilePath + "/allTraining-r-00000.avro"
        self.testDataFile =  self.trainingFilePath + "/allTest-r-00001.avro"

        self.scoreFile = self.modelDirectoryPath +  "/ScoreFile.csv"

        # feature datatype file
        self.varType_file = self.configParametersPath +"/varType.csv"

        #file that has list of features expected to be in the rfmodel file
        self.expectFeat_file = self.configParametersPath + "/ExpectedFeatureList.csv"

        # file that establishes the limits for the features expected to be in the rfmodel file
        self.expectFeatLimit_file = self.configParametersPath + "/ExpectedFeatureLimits.csv"

        # Country grouping from the following file
        self.countryGrouping_file = self.configParametersPath +"/CountryRegionMapping.csv"


        # change the feature name
        self.source_cols = {'AlexaRank': 0.6, 'Semrush': 0.6, 'CloudTech': 0.3, 'BusinessTech': 0.65, 'Feature': 0.50,  'LE_EMPLOYEE_RANGE': 0.40}

        self.source_et = {'AlexaRank':'Alexa','SemrushRank':'Semrush', 'CloudTechnologies_DataCenterSolutions_One':'HG Data','BusinessTechnologiesAnalytics':'BuiltWith','FeatureAverageDocumentSizeProcessed':'LE_Scraping','LE_EMPLOYEE_RANGE': 'Firmographics'}

        self.source_tp = {'AlexaRank':'Alexa','Semrush':'Semrush', 'CloudTech':'HG Data','BusinessTech':'BuiltWith','Feature':'LE_Scraping','LE_EMPLOYEE_RANGE': 'Firmographics'}

        self.no_data = ['Not Available','NULL','[null]', '["null"]', '', '["Not Available"]']
        self.dom_Features = ['CloudTech','BusinessTech']
        self.fundamentalType = ['numeric','currency','year']

        #Event table columns used
        self.publicDomain = 'IsPublicDomain'
        self.emailColumn = ['Email','Website']
        self.eventColName = 'Event'
        self.eventCol = 'Event'
        self.companyColumn = 'CompanyName'
        self.companyColumnAlt = 'Company'

        self.employeeRangecolumn = 'LE_EMPLOYEE_RANGE'

        self.matchRateColumn = 'AlexaRank'
        self.matchRateColumnAlt = 'AlexaViewsRank'
        self.HGmatchRateColumn = 'CloudTechnologies_DataCenterSolutions_One'
        self.HGmatchRateColumnAlt = 'CloudTechnologies_NetworkManagementSoftware'
        self.BWmatchRateColumn = 'BusinessTechnologiesAnalytics'
        self.BWmatchRateColumnAlt = 'BusinessTechnologiesJavascript'

        self.DNBmatchRateColumn = 'LE_EMPLOYEE_RANGE'
        self.DNBmatchRateColumnAlt = 'LE_REVENUE_RANGE'

        self.leCountryColName = 'LE_COUNTRY'
        self.countryColName = 'Country'

        self.dummyCOlName = 'dummyColumn'

        self.countryColumns = ['Country','BusinessCountry']
        self.idColumn = 'Id'

        #Json file column
        self.scoreCol = 'Score'
        self.countCol = 'Count'
        self.convertedCol = 'Converted'

        # expected top level domains with the maximum number of leads
        self.topLeveldomain = ['.com','.org', '.co', '.us']

        # rf model feature limits
        self.featureImp_higherLim = 0.05
        self.featureImp_lowerLim = 0.012

        # highly positively preditive small population limits
        self.numBucket = 10
        self.highly_positively_predictive_small_population_popPerc_threshold = 0.01
        self.highly_positively_predictive_small_population_lift_threshold = 2

        self.rowDelallowed = 0.1
        self.eventDelallowed = 0.1
        self.eventMinumum = 300

        # companySize grouping
        self.companySizegroups = {'': 'N/A',
                         '1 - 10': 'SMB',
                         '10 - 50': 'SMB',
                         '200 - 500': 'MIDMARKET',
                         '50 - 200': 'MIDMARKET',
                         '101-200': 'MIDMARKET',
                         '500 - 1,000': 'ENT',
                         '1,000 - 5,000': 'ENT',
                         '5,000 - 10,000': 'ENT',
                         '> 10,000': 'ENT'}

        # Grouping limits
        self.liftLowerlimit = 0.5
        self.liftUpperlimit = 2
        self.subPoplowerLimit = 5000
        self.subPopminPercent = 0.2
        self.minMatchratediff = 0.2
        self.minEventCnt = 300

        # Index explanation
        self.topPredictor_cols = {0: 'Original Column Name', 1: 'Attribute Name',2: 'Category',3: 'FundamentalType',4: 'Predictive Power',5: 'Attribute Value', 6: 'Conversion Rate', 7:'Lift', 8:'Total Leads', 9: 'Frequency(#)', 10:'Frequency/Total Leads', 11:'ApprovedUsage', 12:'Tags', 13:'StatisticalType', 14:'Attribute Description', 15:'DataSource'}

        self.rfmodel_cols = {0: 'Column Name', 1: ' Feature Importance'}

        self.expectedFeature_cols = {0: 'Expected Top Features', 1: 'FundamentalType'}

        self.expectedFeatureLimits_cols = {0:'Expected Top Features', 1:'num/cat', 2:'Pos/Neg', 3:'criteria 1 (population perc and lift)',4:'1_PopulationPercent',5:'1_Lift', 6:'criteria 2 (Trend metric)', 7:'criteria 3 (bias)', 8:'3_Lift', 9:'Flag', 10:'Comment'}

        ###########################################################################
        # Sriteja's parameters below
        ###########################################################################

        #variales for results of model review file
        #Output file path and name
        #column names in the output file
        #result_Columnnames = ['Issue type','Feature','Pass/Fail','Information']


        #variables for source files

        '''variables for spam indicator column names in the event table. It defines the column name and corresponding value for which we are
        calculating lift.
        '''

        self.SpamIndicators_LiftSeg = {'Domain_Length':0 ,'CompanyName_Length':0, 'CompanyName_Entropy':0, 'Title_Length':0}
        #lift threshold to flag
        self.lift_limit_SpamIndicators = 1


        '''Variable which has the list of column name for which Lift comparison study is done
        ( i,e higher values of the column correspond to higher lift values)'''

        self.SpamIndicators_LiftCompar = ['Domain_Length', 'CompanyName_Length', 'Title_Length']

        #Variables for column names from top predictor files used to perform lift comparison
        self.TopPredictor_Attributebuckets = 'Attribute Value'
        self.TopPredcitor_ColumnName = 'Original Column Name'
        self.TopPredictor_LiftColumn = 'Lift'

        #List of internal features
        self.Int_Feat = ['Title','LeadSource', 'Industry']
        self.lift_limit_InternalFeatures = 1

        #Key word to Identify HGData columns
        self.HG_key = 'CloudTechnologies_'
        self.lift_limit_HGData = 1

        #Key words to Identify Built With columns
        self.Alexa_key = 'Alexa_'
        self.Semrush_key = 'Semrush'
        self.BW_key = 'BusinessTechnologies'
        self.lift_limit_BWData = 1

        #Key words to Identify Built With columns
        self.FeatureTerm_Key = 'FeatureTerm'
        self.lift_limit_FeatureTerm = 1

        #Key word to identify HPANumPages column
        self.HPANum_key = 'HPANumPages'

        self.interFeatPopshrd = 0.04
        self.interFeatLiftUB = 3
        self.interFeatLiftLB = 0.33
