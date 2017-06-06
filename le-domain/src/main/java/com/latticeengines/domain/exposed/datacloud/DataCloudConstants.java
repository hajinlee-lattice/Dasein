package com.latticeengines.domain.exposed.datacloud;

import java.text.SimpleDateFormat;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class DataCloudConstants {

    public static final String SERVICE_TENANT = "DataCloudService";
    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse(SERVICE_TENANT).toString();

    public static final String ACCOUNT_MASTER_COLUMN = "AccountMasterColumn";
    public static final String ACCOUNT_MASTER = "AccountMaster";

    public static final String ATTR_CATEGORY = "Category";
    public static final String ATTR_SUB_CATEGORY = "SubCategory";
    public static final String ATTR_COUNTRY = "LDC_Country";
    public static final String ATTR_INDUSTRY = "LE_INDUSTRY";
    public static final String ATTR_NUM_EMP_RANGE = "LE_EMPLOYEE_RANGE";
    public static final String ATTR_REV_RANGE = "LE_REVENUE_RANGE";
    public static final String ATTR_NUM_LOC_RANGE = "LE_NUMBER_OF_LOCATIONS";
    public static final String BUCKETED_ACCOUNT_MASTER_TABLE_NAME = "AccountMasterBucketed";

    public static final String PROFILE_ATTR_ATTRNAME = "AttrName";
    public static final String PROFILE_ATTR_SRCATTR = "SrcAttr";
    public static final String PROFILE_ATTR_DECSTRAT = "DecodeStrategy";
    public static final String PROFILE_ATTR_ENCATTR = "EncAttr";
    public static final String PROFILE_ATTR_LOWESTBIT = "LowestBit";
    public static final String PROFILE_ATTR_NUMBITS = "NumBits";
    public static final String PROFILE_ATTR_BKTALGO = "BktAlgo";

    public static final String STATS_ATTR_NAME = "AttrName";
    public static final String STATS_ATTR_COUNT = "AttrCount";
    public static final String STATS_ATTR_BKTS = "BktCounts";
    public static final String STATS_ATTR_ALGO = PROFILE_ATTR_BKTALGO;

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_z");
}
