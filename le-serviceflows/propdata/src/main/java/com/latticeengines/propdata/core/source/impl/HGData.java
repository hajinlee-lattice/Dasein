package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component
public class HGData implements DomainBased, DerivedSource, HasSqlPresence {

    private static final long serialVersionUID = 603829385601451984L;

    @Value("${propdata.job.hgdata.refresh.schedule:}")
    private String cronExpression;

    @Autowired
    private HGDataRaw baseSource;

    @Override
    public String getSourceName() {
        return "HGData";
    }

    @Override
    public String getSqlTableName() {
        return "HGData_Source";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain", "Supplier_Name", "Segment_Name", "HG_Category_1", "HG_Category_2",
                "HG_Category_1_Parent", "HG_Category_2_Parent" };
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getDomainField() {
        return "Domain";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public String getSqlMatchDestination() {
        return "HGData_Source";
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NEVER;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return null;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return null;
    }

}