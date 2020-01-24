package com.latticeengines.domain.exposed.dataflow.operations;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

/**
 * To build operation logs in dataflow. Currently only DataCloud pipeline uses
 * it. But it could be used in any dataflow. So put in the package not specific
 * to DataCloud
 */
public final class OperationMessage {

    protected OperationMessage() {
        throw new UnsupportedOperationException();
    }
    // Commonly shared message
    public static final String HAS_DUNS = "Has Duns";
    public static final String HAS_DU_DUNS = "Has DuDuns";
    public static final String IS_DU_DUNS = "DU account with Duns=DUDuns";
    public static final String IS_GU_DUNS = "GU account with Duns=GUDuns";
    public static final String RANDOM = "Random selection";

    // AccountMasterSeed primary domain per duns/du duns selection
    public static final String DOMAIN_ONLY = "Domain-only Entry";
    public static final String DOMAIN_SRC = "DomainSource=%s";
    public static final String LOW_ALEXA_RANK = "Lower AlexaRank=%d";
    public static final String ORI_PRIDOM_FLAG = "Original " + DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN + "=%s";
    public static final String DU_DOMAIN = "DU domain";
    public static final String HIGH_OCCUR = "Higher occurrence=%d";

    // AccountMasterSeed filter FLAG_DROP_OOB_ENTRY == 0
    public static final String NON_OOB = "Non-OOB";

    // Primary account marked by manual seed
    public static final String PRIMARY_ACCOUNT = "Manual seed primary account";
    public static final String NON_PRIMARY_ACCOUNT = "Not manual seed primary account";

    // AccountMasterSeed primary location selection
    public static final String LARGER_VAL_WITH_THRES = "Larger %s=%s(>=threshold %s) && gap=%s(>=%s)";
    public static final String LARGER_VAL = "Larger %s=%s";
    public static final String ORI_PRILOC_FLAG = "Original " + DataCloudConstants.ATTR_IS_PRIMARY_LOCATION + "=%s";
    public static final String USA_ACT = "USA account";

    // Domain ownership table
    public static final String CLEAN_NONOWNER_DOM = "Owned by duns %s with reason %s";
    public static final String REPLACE_SECDOM_WITH_PRIDOM = "%s is orb sec domain";

}
