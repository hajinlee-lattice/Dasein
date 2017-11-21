package com.latticeengines.domain.exposed.datacloud;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public final class DataCloudConstants {

    public static final String SERVICE_TENANT = "DataCloudService";
    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse(SERVICE_TENANT).toString();

    public static final String ACCOUNT_MASTER_COLUMN = "AccountMasterColumn";
    public static final String ACCOUNT_MASTER = "AccountMaster";
    public static final String ACCOUNT_MASTER_COLLECTION = "AMCollection";

    public static final String LATTIC_ID = "LatticeID";
    public static final String LATTICE_ACCOUNT_ID = InterfaceName.LatticeAccountId.name();

    public static final String ATTR_CATEGORY = "Category";
    public static final String ATTR_SUB_CATEGORY = "SubCategory";
    public static final String ATTR_COUNTRY = "LDC_Country";
    public static final String ATTR_INDUSTRY = "LE_INDUSTRY";
    public static final String ATTR_NUM_EMP_RANGE = "LE_EMPLOYEE_RANGE";
    public static final String ATTR_REV_RANGE = "LE_REVENUE_RANGE";
    public static final String ATTR_NUM_LOC_RANGE = "LE_NUMBER_OF_LOCATIONS";
    public static final String ATTR_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    public static final String ATTR_LDC_NAME = "LDC_Name";
    public static final String ATTR_LDC_DOMAIN = "LDC_Domain";
    public static final String ATTR_LDC_INDUSTRY = "LDC_PrimaryIndustry";

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

    public static final String TRANSFORMER_AM_DECODER = "AMDecoder";
    public static final String TRANSFORMER_AM_ENRICHER = "AMAttrEnricher";
    public static final String TRANSFORMER_PROFILER = "SourceProfiler";
    public static final String TRANSFORMER_BUCKETER = "sourceBucketer";
    public static final String TRANSFORMER_BUCKETED_FILTER = "bucketedFilter";
    public static final String TRANSFORMER_STATS_CALCULATOR = "statsCalculator";
    public static final String TRANSFORMER_SORTER = "sourceSorter";
    public static final String TRANSFORMER_COPIER = "sourceCopier";
    public static final String TRANSFORMER_MATCH = "bulkMatchTransformer";
    public static final String TRANSFORMER_TRANSACTION_AGGREGATOR = "transactionAggregator";

    public static final String TRANSFORMER_CONSOLIDATE_PARTITION = "ConsolidatePartition";
    public static final String TRANSFORMER_CONSOLIDATE_RETAIN = "consolidateRetainFieldTransformer";
    public static final String PERIOD_DATA_CLEANER = "periodDataCleaner";
    public static final String PERIOD_DATA_FILTER = "periodDatafilter";
    public static final String PERIOD_DATA_AGGREGATER = "periodDataAggregater";
    public static final String PERIOD_DATA_DISTRIBUTOR = "periodDataDistributor";
    public static final String PERIOD_COLLECTOR = "periodCollector";
    public static final String PERIOD_CONVERTOR = "periodConvertor";
    public static final String PERIOD_DATE_CONVERTOR = "periodDateConvertor";
    public static final String PRODUCT_MAPPER = "productMapper";

}
