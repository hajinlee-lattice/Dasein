package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("hgDataTechIndicators")
public class HGDataTechIndicators implements DerivedSource, DomainBased {

    private static final long serialVersionUID = 603829385601451981L;

    @Autowired
    private HGDataClean hgDataClean;

    @Override
    public Source[] getBaseSources() {
        return new Source[]{ hgDataClean };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "HGDataTechIndicators";
    }

    @Override
    public String getTimestampField() {
        return "Timestamp";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain" };
    }

    @Override
    public String getDomainField() {
        return "Domain";
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

}
