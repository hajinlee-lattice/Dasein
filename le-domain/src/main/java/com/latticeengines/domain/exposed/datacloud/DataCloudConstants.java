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
    public static final String ATTR_LDC_NAME = "LDC_Name";
    public static final String ATTR_LDC_DOMAIN = "LDC_Domain";

    public static final String PROFILE_STAGE_SEGMENT = "SEGMENT";
    public static final String PROFILE_STAGE_ENRICH = "ENRICH";
    public static final String PROFILE_ATTR_ATTRNAME = "AttrName";
    public static final String PROFILE_ATTR_SRCATTR = "SrcAttr";
    public static final String PROFILE_ATTR_DECSTRAT = "DecodeStrategy";
    public static final String PROFILE_ATTR_ENCATTR = "EncAttr";
    public static final String PROFILE_ATTR_LOWESTBIT = "LowestBit";
    public static final String PROFILE_ATTR_NUMBITS = "NumBits";
    public static final String PROFILE_ATTR_BKTALGO = "BktAlgo";

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
    public static final String TRANSFORMER_STATS_CALCULATOR = "statsCalculator";
    public static final String TRANSFORMER_SORTER = "sourceSorter";
    public static final String TRANSFORMER_MATCH = "bulkMatchTransformer";
    
    public static final String TRANSFORMER_CONSOLIDATE_PARTITION= "ConsolidatePartition";
}
