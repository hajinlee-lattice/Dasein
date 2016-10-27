package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.Date;

import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;

public class HGDataCleanConfiguration extends BasicTransformationConfiguration implements TransformationConfiguration {

    private Date fakedCurrentDate;

    public Date getFakedCurrentDate() {
        return fakedCurrentDate;
    }

    public void setFakedCurrentDate(Date fakedCurrentDate) {
        this.fakedCurrentDate = fakedCurrentDate;
    }
}
