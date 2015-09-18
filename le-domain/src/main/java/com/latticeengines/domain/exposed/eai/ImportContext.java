package com.latticeengines.domain.exposed.eai;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.BaseContext;

public class ImportContext extends BaseContext {

    public ImportContext(Configuration yarnConfiguration){
        super(yarnConfiguration);
    }

}
