package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.RefreshedSource;
import com.latticeengines.propdata.core.source.Source;

@Component("bomboraDomain")
public class BomboraDomain implements RefreshedSource {

    private static final long serialVersionUID = 8295321788746751057L;

    @Autowired
    private BomboraDepivoted bomboraDepivoted;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { bomboraDepivoted };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "BomboraDomain";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "ID" };
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
