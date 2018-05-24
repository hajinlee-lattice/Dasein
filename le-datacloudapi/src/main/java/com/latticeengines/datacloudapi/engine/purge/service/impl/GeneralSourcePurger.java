package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("generalSourcePurger")
public class GeneralSourcePurger extends VersionedPurger {
    @Override
    protected SourceType getSourceType() {
        return SourceType.GENERAL_SOURCE;
    }
}
