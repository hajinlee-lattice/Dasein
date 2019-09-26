package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.AccountContactExportContext;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AccountContactExportConfig extends SparkJobConfig {

    /**
     *
     */
    private static final long serialVersionUID = 6074642948393476582L;

    public static final String NAME = "exportAccountContact";

    public static final String CONTACT_ATTR_PREFIX = "ContactRenamed_";

    @JsonProperty("AccountContactExportContext")
    private AccountContactExportContext accountContactExportContext;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public AccountContactExportContext getAccountContactExportContext() {
        return accountContactExportContext;
    }

    public void setAccountContactExportContext(AccountContactExportContext accountContactExportContext) {
        this.accountContactExportContext = accountContactExportContext;
    }

}
