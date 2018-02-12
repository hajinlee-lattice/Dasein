import os
class revParam(object):
    currentPath = os.getcwd()
    head, tail = os.path.split(currentPath)
    filePath = os.path.join(head, 'DSModelReview/')
    scriptsPath = os.path.join(head, 'DSScriptAndParams/')

    topPredictor_file = filePath + "TopPredictors.csv"

    # Dont have to change, file names are same for all models
    modelsummaryJson_file = filePath+ "modelsummary.json"
    rfmodel_file = filePath+ "rf_model.csv"

    output_file = filePath + "Flags.xlsx"

     # Event tablefiles
    trainDataFile = filePath+ "Training.csv"
    testDataFile = filePath+ "Testing.csv"

    scoreFile = filePath+ "ScoreFile.csv"

    # feature datatype file
    varType_file = scriptsPath+"varType.csv"

    #file that has list of features expected to be in the rfmodel file
    expectFeat_file = scriptsPath+"ExpectedFeatureList.csv"

    # file that establishes the limits for the features expected to be in the rfmodel file
    expectFeatLimit_file = scriptsPath+"ExpectedFeatureLimits.csv"

    # Country grouping from the following file
    countryGrouping_file = scriptsPath+"CountryRegionMapping.csv"


   # change the feature name
    source_cols = {'AlexaRank': 0.6, 'Semrush': 0.6, 'CloudTech': 0.3, 'BusinessTech': 0.65, 'Feature': 0.50,  'LE_EMPLOYEE_RANGE': 0.40}

    source_et = {'AlexaRank':'Alexa','SemrushRank':'Semrush', 'CloudTechnologies_DataCenterSolutions_One':'HG Data','BusinessTechnologiesAnalytics':'BuiltWith','FeatureAverageDocumentSizeProcessed':'LE_Scraping','LE_EMPLOYEE_RANGE': 'Firmographics'}

    source_tp = {'AlexaRank':'Alexa','Semrush':'Semrush', 'CloudTech':'HG Data','BusinessTech':'BuiltWith','Feature':'LE_Scraping','LE_EMPLOYEE_RANGE': 'Firmographics'}

    no_data = ['Not Available','NULL','[null]', '["null"]', '', '["Not Available"]']
    dom_Features = ['CloudTech','BusinessTech']
    fundamentalType = ['numeric','currency','year']

    #Event table columns used
    publicDomain = 'IsPublicDomain'
    emailColumn = ['Email','Website']
    eventColName = 'Event'
    eventCol = 'Event'
    companyColumn = 'CompanyName'
    companyColumnAlt = 'Company'

    employeeRangecolumn = 'LE_EMPLOYEE_RANGE'

    matchRateColumn = 'AlexaRank'
    matchRateColumnAlt = 'AlexaViewsRank'
    HGmatchRateColumn = 'CloudTechnologies_DataCenterSolutions_One'
    HGmatchRateColumnAlt = 'CloudTechnologies_NetworkManagementSoftware'
    BWmatchRateColumn = 'BusinessTechnologiesAnalytics'
    BWmatchRateColumnAlt = 'BusinessTechnologiesJavascript'

    DNBmatchRateColumn = 'LE_EMPLOYEE_RANGE'
    DNBmatchRateColumnAlt = 'LE_REVENUE_RANGE'

    leCountryColName = 'LE_COUNTRY'
    countryColName = 'Country'

    dummyCOlName = 'dummyColumn'

    countryColumns = ['Country','BusinessCountry']
    idColumn = 'Id'

    #Json file column
    scoreCol = 'Score'
    countCol = 'Count'
    convertedCol = 'Converted'

    # expected top level domains with the maximum number of leads
    topLeveldomain = ['.com','.org', '.co', '.us']

    # rf model feature limits
    featureImp_higherLim = 0.05
    featureImp_lowerLim = 0.012

    # highly positively preditive small population limits
    numBucket = 10
    highly_positively_predictive_small_population_popPerc_threshold = 0.01
    highly_positively_predictive_small_population_lift_threshold = 2

    rowDelallowed = 0.1
    eventDelallowed = 0.1
    eventMinumum = 300

    # companySize grouping
    companySizegroups = {'': 'N/A',
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
    liftLowerlimit = 0.5
    liftUpperlimit = 2
    subPoplowerLimit = 5000
    subPopminPercent = 0.2
    minMatchratediff = 0.2
    minEventCnt = 300

    # Index explanation
    topPredictor_cols = {0: 'Original Column Name', 1: 'Attribute Name',2: 'Category',3: 'FundamentalType',4: 'Predictive Power',5: 'Attribute Value', 6: 'Conversion Rate', 7:'Lift', 8:'Total Leads', 9: 'Frequency(#)', 10:'Frequency/Total Leads', 11:'ApprovedUsage', 12:'Tags', 13:'StatisticalType', 14:'Attribute Description', 15:'DataSource'}

    rfmodel_cols = {0: 'Column Name', 1: ' Feature Importance'}

    expectedFeature_cols = {0: 'Expected Top Features', 1: 'FundamentalType'}

    expectedFeatureLimits_cols = {0:'Expected Top Features', 1:'num/cat', 2:'Pos/Neg', 3:'criteria 1 (population perc and lift)',4:'1_PopulationPercent',5:'1_Lift', 6:'criteria 2 (Trend metric)', 7:'criteria 3 (bias)', 8:'3_Lift', 9:'Flag', 10:'Comment'}

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
    SpamIndicators_LiftSeg = {'Domain_Length':0 ,'CompanyName_Length':0, 'CompanyName_Entropy':0, 'Title_Length':0}
    #lift threshold to flag
    lift_limit_SpamIndicators = 1


    '''Variable which has the list of column name for which Lift comparison study is done
    ( i,e higher values of the column correspond to higher lift values)'''

    SpamIndicators_LiftCompar = ['Domain_Length', 'CompanyName_Length', 'Title_Length']

    #Variables for column names from top predictor files used to perform lift comparison
    TopPredictor_Attributebuckets = 'Attribute Value'
    TopPredcitor_ColumnName = 'Original Column Name'
    TopPredictor_LiftColumn = 'Lift'

    #List of internal features
    Int_Feat = ['Title','LeadSource', 'Industry']
    lift_limit_InternalFeatures = 1

    #Key word to Identify HGData columns
    HG_key = 'CloudTechnologies_'
    lift_limit_HGData = 1

    #Key words to Identify Built With columns
    Alexa_key = 'Alexa_'
    Semrush_key = 'Semrush'
    BW_key = 'BusinessTechnologies'
    lift_limit_BWData = 1

    #Key words to Identify Built With columns
    FeatureTerm_Key = 'FeatureTerm'
    lift_limit_FeatureTerm = 1

    #Key word to identify HPANumPages column
    HPANum_key = 'HPANumPages'

    interFeatPopshrd = 0.04
    interFeatLiftUB = 3
    interFeatLiftLB = 0.33


