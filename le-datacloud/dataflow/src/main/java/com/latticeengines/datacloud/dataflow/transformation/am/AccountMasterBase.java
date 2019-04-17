package com.latticeengines.datacloud.dataflow.transformation.am;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public abstract class AccountMasterBase<T extends TransformerConfig> extends ConfigurableFlowBase<T> {

    // ========================================
    // Column Name Constants
    //
    // do not make them public
    // it is only for keeping the lineage
    // within AM build flows
    // ========================================

    // Internal Columns, not visible outside of AM Dataflows. It is safe to keep lineage here.
    protected static final String FLAG_DROP_OOB_ENTRY = "_FLAG_DROP_OOB_ENTRY_";
    protected static final String FLAG_DROP_SMALL_BUSINESS = "_FLAG_DROP_SMALL_BUSINESS_";
    protected static final String FLAG_DROP_INCORRECT_DATA = "_FLAG_DROP_INCORRECT_DATA_";
    protected static final String FLAG_DROP_LESS_POPULAR_DOMAIN = "_FLAG_DROP_LESS_POPULAR_DOMAIN_";
    protected static final String FLAG_DROP_ORPHAN_ENTRY = "_FLAG_DROP_ORPHAN_ENTRY_";

    // External Columns, those should be migrated to configuration json, as they may change without deploying new code
    protected static final String DUNS = "DUNS";
    protected static final String DOMAIN = "Domain";
    protected static final String LATTICE_ID = "LatticeID";
    protected static final String LE_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    protected static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    protected static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    protected static final String SALES_VOLUME_US_DOLLARS = "SALES_VOLUME_US_DOLLARS";
    protected static final String ALEXA_RANK = "Rank";
    protected static final String ALEXA_URL = "URL";
    protected static final String ALEXA_RANK_AMSEED = "AlexaRank";
    protected static final String COUNTRY = "Country";
    protected static final String LE_EMPLOYEE_RANGE = "LE_EMPLOYEE_RANGE";
    protected static final String OUT_OF_BUSINESS_INDICATOR = "OUT_OF_BUSINESS_INDICATOR";
    protected static final String DOMAIN_SOURCE = "DomainSource";
    protected static final String PRIMARY_DOMAIN = "PrimaryDomain";
    protected static final String SECONDARY_DOMAIN = "SecondaryDomain";
    protected static final String LE_PRIMARY_DUNS = "LE_PRIMARY_DUNS";
}
