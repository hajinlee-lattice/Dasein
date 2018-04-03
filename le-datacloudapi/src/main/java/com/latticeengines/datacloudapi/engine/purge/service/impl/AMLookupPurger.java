package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("amLookupPurger")
public class AMLookupPurger extends AMSourcePurger {

    @Override
    public SourceType getSourceType() {
        return SourceType.ACCOUNT_MASTER_LOOKUP;
    }

    @Override
    protected String getHdfsVersionFromDCVersion(DataCloudVersion dcVersion) {
        return dcVersion.getAccountLookupHdfsVersion();
    }
}
