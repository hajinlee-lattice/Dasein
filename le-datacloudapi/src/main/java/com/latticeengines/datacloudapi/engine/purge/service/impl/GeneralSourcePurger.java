package com.latticeengines.datacloudapi.engine.purge.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * Targeting source: DataCloud Source
 *
 * When to purge a version:
 *
 * When a version is older than most recent
 * {@link #PurgeStrategy.getHdfsVersions}} number of versions (if provided) AND
 * its created time is older than {@link #PurgeStrategy.getHdfsDays}} number of
 * days (if provided).
 */
@Component("generalSourcePurger")
public class GeneralSourcePurger extends VersionedPurger {
    @Override
    protected SourceType getSourceType() {
        return SourceType.GENERAL_SOURCE;
    }
}
