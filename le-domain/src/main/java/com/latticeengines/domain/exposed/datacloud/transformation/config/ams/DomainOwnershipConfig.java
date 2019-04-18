package com.latticeengines.domain.exposed.datacloud.transformation.config.ams;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class DomainOwnershipConfig extends TransformerConfig {

    public static final String ROOT_DUNS = "ROOT_DUNS";
    public static final String DUNS_TYPE = "DUNS_TYPE";
    public static final String TREE_NUMBER = "TREE_NUMBER";
    public static final String REASON_TYPE = "REASON_TYPE";
    public static final String ORB_SEC_PRI_DOMAIN = "PrimaryDomain";
    public static final String ORB_SRC_SEC_DOMAIN = "SecondaryDomain";
    public static final String PRIMARY_ROOT_DUNS = "PRIMARY_ROOT_DUNS";
    public static final String SECONDARY_ROOT_DUNS = "SECONDARY_ROOT_DUNS";
    public static final String MISSING_ROOT_DUNS = "MISSING_ROOT_DUNS";
    public static final String TREE_ROOT_DUNS = "TREE_ROOT_DUNS";
    public static final String IS_NON_PROFITABLE = "IS_NON_PROFITABLE";
    public static final String DOM_OWNERSHIP_TABLE = "DomainOwnershipTable";
    public static final String ORB_SEC_CLEANED = "OrbSecCleaned";
    public static final String AMS_CLEANED = "AmsCleaned";

    @JsonProperty("FranchiseThreshold")
    private int franchiseThreshold;

    @JsonProperty("MultLargeCompThreshold")
    private Long multLargeCompThreshold;

    public int getFranchiseThreshold() {
        return franchiseThreshold;
    }

    public void setFranchiseThreshold(int franchiseThreshold) {
        this.franchiseThreshold = franchiseThreshold;
    }

    public Long getMultLargeCompThreshold() {
        return multLargeCompThreshold;
    }

    public void setMultLargeCompThreshold(Long multLargeCompThreshold) {
        this.multLargeCompThreshold = multLargeCompThreshold;
    }

}
