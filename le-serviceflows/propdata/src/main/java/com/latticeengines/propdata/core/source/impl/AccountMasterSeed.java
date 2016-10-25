package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("accountMasterSeed")
public class AccountMasterSeed implements DerivedSource {

    private static final long serialVersionUID = -3119903346347156027L;

    @Autowired
    LatticeCacheSeed latticeCacheSeed;

    @Autowired
    DnBCacheSeed dnBCacheSeed;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { dnBCacheSeed, latticeCacheSeed };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "AccountMasterSeed";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeID" };
    }

    @Override
    public String getDefaultCronExpression() {
        return null;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 1;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return null;
    }

    public String getDomainField() {
        return "Domain";
    }

    public String getDunsField() {
        return "DUNS";
    }

    public String[] getRetainFields() {
        return new String[] { "LatticeID", "DUNS", "Domain", "Name", "Street", "City", "State", "Country", "ZipCode",
                "LE_IS_PRIMARY_DOMAIN", "LE_IS_PRIMARY_LOCATION", "LE_NUMBER_OF_LOCATIONS", "LE_PRIMARY_DUNS",
                "LE_COMPANY_DESCRIPTION", "LE_COMPANY_PHONE", "LE_SIC_CODE", "LE_NAICS_CODE", "LE_INDUSTRY",
                "LE_REVENUE_RANGE", "LE_EMPLOYEE_RANGE", "LE_COUNTRY" };
    }
}
