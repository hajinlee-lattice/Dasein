package com.latticeengines.propdata.engine.transformation.configuration.impl;

import java.util.Date;

import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public class HGDataCleanConfiguration extends BasicTransformationConfiguration implements TransformationConfiguration {

    private Date fakedCurrentDate;

    public Date getFakedCurrentDate() {
        return fakedCurrentDate;
    }

    public void setFakedCurrentDate(Date fakedCurrentDate) {
        this.fakedCurrentDate = fakedCurrentDate;
    }
}
