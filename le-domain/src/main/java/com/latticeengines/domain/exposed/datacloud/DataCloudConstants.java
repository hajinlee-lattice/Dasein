package com.latticeengines.domain.exposed.datacloud;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public final class DataCloudConstants {

    public static final String SERVICE_TENANT = "DataCloudService";
    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse(SERVICE_TENANT)
            .toString();

    public static final String ACCOUNT_MASTER_COLUMN = "AccountMasterColumn";
    public static final String ACCOUNT_MASTER = "AccountMaster";
    public static final String ACCOUNT_MASTER_COLLECTION = "AMCollection";
    public static final String PATCH_BOOK_ATTR_PREFIX = "PATCH_";

    public static final String LATTICE_ID = "LatticeID";
    public static final String LATTICE_ACCOUNT_ID = InterfaceName.LatticeAccountId.name();

    // entity match constants
    public static final String ENTITY_PREFIX_LOOKUP = "LOOKUP";
    public static final String ENTITY_PREFIX_SEED = "SEED";
    public static final String ENTITY_PREFIX_VERSION = "VERSION"; // entity match version prefix
    public static final String ENTITY_PREFIX_SEED_ATTRIBUTES = "ATTRS_";
    public static final String ENTITY_DELIMITER = "_";
    public static final String ENTITY_ANONYMOUS_ID = "__ANONYMOUS__";
    // Anonymous AccountId
    public static final String ENTITY_ANONYMOUS_AID = "__ANONYMOUS_AID__";
    // entity match attribute names
    public static final String ENTITY_ATTR_PID = "PID"; // primary id
    public static final String ENTITY_ATTR_SID = "SID"; // secondary id
    public static final String ENTITY_ATTR_SEED_ID = "SeedId";
    public static final String ENTITY_ATTR_ENTITY = "Entity";
    public static final String ENTITY_ATTR_VERSION = "Version";
    public static final String ENTITY_ATTR_EXPIRED_AT = "ExpiredAt";

    // AM attribute names
    public static final String ATTR_CATEGORY = "Category";
    public static final String ATTR_SUB_CATEGORY = "SubCategory";
    public static final String ATTR_COUNTRY = "LDC_Country";
    public static final String ATTR_STATE = "LDC_State";
    public static final String ATTR_CITY = "LDC_City";
    public static final String ATTR_ZIPCODE = "LDC_ZipCode";
    public static final String ATTR_INDUSTRY = "LE_INDUSTRY";
    public static final String ATTR_NUM_EMP_RANGE = "LE_EMPLOYEE_RANGE";
    public static final String ATTR_NUM_EMP_RANGE_LABEL = "LE_EMPLOYEE_RANGE_LABEL";
    public static final String ATTR_REV_RANGE = "LE_REVENUE_RANGE";
    public static final String ATTR_NUM_LOC_RANGE = "LE_NUMBER_OF_LOCATIONS";
    public static final String ATTR_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    public static final String ATTR_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    public static final String ATTR_LDC_NAME = "LDC_Name";
    public static final String ATTR_LDC_DOMAIN = "LDC_Domain";
    public static final String ATTR_LDC_DUNS = "LDC_DUNS";
    public static final String ATTR_LDC_DOMAIN_SOURCE = "LDC_DomainSource";
    public static final String ATTR_LDC_INDUSTRY = "LDC_PrimaryIndustry";
    public static final String ATTR_IS_PRIMARY_ACCOUNT = "IsPrimaryAccount";
    public static final String ATTR_LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    public static final String ATTR_DU_DUNS = "LE_PRIMARY_DUNS";
    public static final String ATTR_GU_DUNS = "GLOBAL_ULTIMATE_DUNS_NUMBER";
    public static final String ATTR_SALES_VOL_US = "SALES_VOLUME_US_DOLLARS";
    public static final String ATTR_SALES_VOL_US_CODE = "SALES_VOLUME_RELIABILITY_CODE";
    public static final String ATTR_GLOBAL_HQ_SALES_VOL = "GLOBAL_HQ_SALES_VOLUME";
    public static final String ATTR_EMPLOYEE_HERE = "EMPLOYEES_HERE";
    public static final String ATTR_EMPLOYEE_HERE_CODE = "EMPLOYEES_HERE_RELIABILITY_CODE";
    public static final String ATTR_EMPLOYEE_TOTAL = "EMPLOYEES_TOTAL";
    public static final String ATTR_EMPLOYEE_TOTAL_CODE = "EMPLOYEES_TOTAL_RELIABILITY_CODE";
    public static final String ATTR_IS_CTRY_PRIMARY_LOCATION = "IsCountryPrimaryLocation";
    public static final String ATTR_IS_ST_PRIMARY_LOCATION = "IsStatePrimaryLocation";
    public static final String ATTR_IS_ZIP_PRIMARY_LOCATION = "IsZipPrimaryLocation";
    public static final String ATTR_ALEXA_RANK = "AlexaRank";
    public static final String ATTR_ALEXA_DOMAIN = "URL";
    public static final String ATTR_LAST_UPLOAD_DATE = "LE_Last_Upload_Date";
    public static final String ATTR_PATCH_DOMAIN = PATCH_BOOK_ATTR_PREFIX + PatchBook.COLUMN_DOMAIN;
    public static final String ATTR_PATCH_DUNS = PATCH_BOOK_ATTR_PREFIX + PatchBook.COLUMN_DUNS;
    public static final String ATTR_PATCH_ITEMS = PATCH_BOOK_ATTR_PREFIX + PatchBook.COLUMN_PATCH_ITEMS;

    // AMSeed attribute names which are different with AM
    // As for attributes shared between AMSeed and AM, just use ATTR_xxx
    public static final String AMS_ATTR_DOMAIN = "Domain";
    public static final String AMS_ATTR_DUNS = "DUNS";
    public static final String AMS_ATTR_NAME = "Name";
    public static final String AMS_ATTR_COUNTRY = "Country";
    public static final String AMS_ATTR_STATE = "State";
    public static final String AMS_ATTR_CITY = "City";
    public static final String AMS_ATTR_ZIP = "ZipCode";
    public static final String AMS_ATTR_PRIMARY_INDUSTRY = "PrimaryIndustry";
    public static final String AMS_ATTR_DOMAIN_SOURCE = "DomainSource";

    // Attribute name in sources
    public static final String ALEXA_ATTR_URL = "URL";
    public static final String ALEXA_ATTR_RANK = "Rank";
    public static final String ORBSEC_ATTR_PRIDOM = "PrimaryDomain";
    public static final String ORBSEC_ATTR_SECDOM = "SecondaryDomain";

    public static final String PROFILE_STAGE_SEGMENT = "SEGMENT";
    public static final String PROFILE_STAGE_ENRICH = "ENRICH";
    public static final String PROFILE_ATTR_ATTRNAME = "AttrName";
    public static final String PROFILE_ATTR_SRCATTR = "SrcAttr";
    public static final String PROFILE_ATTR_DECSTRAT = "DecodeStrategy";
    public static final String PROFILE_ATTR_ENCATTR = "EncAttr";
    public static final String PROFILE_ATTR_LOWESTBIT = "LowestBit";
    public static final String PROFILE_ATTR_NUMBITS = "NumBits";
    public static final String PROFILE_ATTR_BKTALGO = "BktAlgo";

    public static final String CHK_ATTR_CHK_CODE = "CheckCode";
    public static final String CHK_ATTR_ROW_ID = "RowId";
    public static final String CHK_ATTR_GROUP_ID = "GroupId";
    public static final String CHK_ATTR_CHK_FIELD = "CheckField";
    public static final String CHK_ATTR_CHK_VALUE = "CheckValue";
    public static final String CHK_ATTR_CHK_MSG = "CheckMessage";

    public static final String CEAttr = "CEAttr";
    public static final String EAttr = "EAttr";

    public static final String STATS_ATTR_NAME = "AttrName";
    public static final String STATS_ATTR_COUNT = "AttrCount";
    public static final String STATS_ATTR_BKTS = "BktCounts";
    public static final String STATS_ATTR_ALGO = PROFILE_ATTR_BKTALGO;


    public static final String TRANSFORMER_AM_ENRICHER = "AMAttrEnricher";
    public static final String TRANSFORMER_PROFILER = "SourceProfiler";
    public static final String TRANSFORMER_BUCKETER = "sourceBucketer";
    public static final String TRANSFORMER_BUCKETED_FILTER = "bucketedFilter";
    public static final String TRANSFORMER_MERGE_MATCH = "mergeMatch";
    public static final String TRANSFORMER_STATS_CALCULATOR = "statsCalculator";
    public static final String TRANSFORMER_SORTER = "sourceSorter";
    public static final String TRANSFORMER_DIFFER = "sourceDiffer";
    public static final String TRANSFORMER_MATCH = "bulkMatchTransformer";
    public static final String TRANSFORMER_TRANSACTION_AGGREGATOR = "transactionAggregator";
    public static final String TRANSFORMER_CONTACT_NAME_CONCATENATER = "contactNameConcatenater";
    public static final String TRANSFORMER_PIVOT_RATINGS = "pivotRatingTransformer";
    public static final String TRANSFORMER_MERGE = "MergeTransformer";
    public static final String TRANSFORMER_STANDARDIZATION = "standardizationTransformer";

    public static final String TRANSFORMER_REMOVE_ORPHAN_CONTACT = "RemoveOrphanContact";
    public static final String TRANSFORMER_COPY_TXMFR = "CopyTxmfr";
    public static final String TRANSFORMER_REPARTITION_TXMFR = "RepartitionTxmfr";
    public static final String TRANSFORMER_UPSERT_TXMFR = "UpsertTxmfr";
    public static final String TRANSFORMER_MERGE_IMPORTS = "MergeImports";
    public static final String TRANSFORMER_CONSOLIDATE_REPORT = "ConsolidateReporter";
    public static final String TRANSFORMER_CONSOLIDATE_DATA = "consolidateDataTransformer";
    public static final String TRANSFORMER_CONSOLIDATE_DELTA = "consolidateDeltaTransformer";
    public static final String TRANSFORMER_CONSOLIDATE_PARTITION = "ConsolidatePartition";
    public static final String TRANSFORMER_ATTRIBUTES_DERIVER = "purchaseAttributesDeriver";
    public static final String TRANSFORMER_EXTRACT_EMBEDDED_ENTITY = "ExtractEmbeddedEntityTable";
    public static final String PERIOD_DATA_CLEANER = "periodDataCleaner";
    public static final String PERIOD_DATA_FILTER = "periodDatafilter";
    public static final String PERIOD_DATA_AGGREGATER = "periodDataAggregater";
    public static final String PERIOD_DATA_DISTRIBUTOR = "periodDataDistributor";
    public static final String PERIOD_COLLECTOR = "periodCollector";
    public static final String PERIOD_CONVERTOR = "periodConvertor";
    public static final String PERIOD_DATE_CONVERTOR = "periodDateConvertor";
    public static final String PRODUCT_MAPPER = "productMapper";
    public static final String ACTIVITY_METRICS_CURATOR = "activityMetricsCurator";
    public static final String ACTIVITY_METRICS_PIVOT = "activityMetricsPivot";
    public static final String TRANSFORMER_NUMBER_OF_CONTACTS = "numberOfContacts";

    public static final String TRANSACTION_STANDARDIZER = "transactionStandardizer";

    /***********************************
     * Transformer names for AM rebuild
     ***********************************/
    public static final String TRANSFORMER_MAP_ATTR = "mapAttribute";
    public static final String TRANSFORMER_AMSEED_MARKER = "AMSeedMarkerTransformer";
    public static final String TRANSFORMER_AMSEED_PRIACT_FIX = "AMSeedPriActFixTransformer";
    public static final String TRANSFORMER_AMS_FILL_DOM_IN_DU = "AMSeedFillDomainInDU";
    public static final String TRANSFORMER_AMS_CLEANBY_DOM_OWNER = "AMSeedCleanByDomainOwner";
    public static final String TRANSFORMER_CLEANER = "AMCleaner";
    public static final String TRANSFORMER_AM_DECODER = "AMDecoder";
    public static final String TRANSFORMER_AM_REFRESH_VER_UPDATER = "amRefreshVersionUpdater";

    public static final String PIPELINE_TEMPSRC_PREFIX = "Pipeline_";

    /*************************************************
     * Constant sets for Domain and Duns Based Sources
     *************************************************/
    private static final String[] DOMAIN_SOURCES = new String[] { "AlexaMostRecent",
            "Bombora30DayAgg", "BomboraSurgePivoted", "BuiltWithPivoted",
            "BuiltWithTechIndicators", "FeaturePivoted", "HGDataPivoted", "HGDataTechIndicators", "HPANewPivoted",
            "OrbIntelligenceMostRecent", "SemrushMostRecent" };
    public static final Set<String> DOMAIN_BASED_SOURCES = new HashSet<>(
            Arrays.asList(DOMAIN_SOURCES));
    private static final String[] DUNS_SOURCES = new String[] { "DnBCacheSeed" };
    public static final Set<String> DUNS_BASED_SOURCES = new HashSet<>(Arrays.asList(DUNS_SOURCES));

    /***********************************
     * Domain sources for AM
     ***********************************/
    public static final String DOMSRC_ORB = "Orb";
    public static final String DOMSRC_DNB = "DnB";
    public static final String DOMSRC_HG = "HG";
    public static final String DOMSRC_RTS = "RTS";

    /***********************************
     * Predefined attribute values
     ***********************************/
    public static final String ATTR_VAL_Y = "Y";
    public static final String ATTR_VAL_N = "N";
    public static final String ATTR_VAL_YES = "Yes";
    public static final String ATTR_VAL_NO = "No";
    public static final String ATTR_VAL_1 = "1";
    public static final String ATTR_VAL_0 = "0";
    public static final String ATTR_VAL_TRUE = "True";
    public static final String ATTR_VAL_FALSE = "False";

    /***********************************
     * Matcher Behavior
     ***********************************/
    public static final int REAL_TIME_MATCH_RECORD_LIMIT = 200;
}
