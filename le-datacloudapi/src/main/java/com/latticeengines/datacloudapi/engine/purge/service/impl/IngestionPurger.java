package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * Targeting source: DataCloud Ingestion
 *
 * When to purge a version:
 *
 * When a version is older than most recent
 * {@link #PurgeStrategy.getHdfsVersions}} number of versions (if provided) AND
 * its created time is older than {@link #PurgeStrategy.getHdfsDays}} number of
 * days (if provided).
 */
@Component("ingestionPurger")
public class IngestionPurger extends VersionedPurger{

    private static Logger log = LoggerFactory.getLogger(IngestionPurger.class);

    @Override
    protected SourceType getSourceType() {
        return SourceType.INGESTION_SOURCE;
    }

    @Override
    public List<String> findAllVersions(PurgeStrategy strategy) {
        try {
            IngestionSource ingestion = new IngestionSource(strategy.getSource());
            return hdfsSourceEntityMgr.getVersions(ingestion);
        } catch (Exception ex) {
            log.error("Fail to get all versions for ingestion " + strategy.getSource(), ex);
        }
        return null;
    }

    @Override
    protected List<String> constructHdfsPaths(PurgeStrategy strategy, List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = hdfsPathBuilder.constructIngestionDir(strategy.getSource(), version).toString();
            hdfsPaths.add(hdfsPath);
        });

        return hdfsPaths;
    }

    @Override
    public boolean isSourceExisted(PurgeStrategy strategy) {
        Source source = new IngestionSource(strategy.getSource());
        return hdfsSourceEntityMgr.checkSourceExist(source);
    }

}
