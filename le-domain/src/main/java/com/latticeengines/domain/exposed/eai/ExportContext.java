package com.latticeengines.domain.exposed.eai;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.BaseContext;

public class ExportContext extends BaseContext {

    public ExportContext(Configuration yarnConfiguration) {
        super(yarnConfiguration);
    }

}
