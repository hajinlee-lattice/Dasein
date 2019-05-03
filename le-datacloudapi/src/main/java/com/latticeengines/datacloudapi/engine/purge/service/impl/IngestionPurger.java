package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

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
            IngestionSource ingestion = new IngestionSource();
            ingestion.setIngestionName(strategy.getSource());
            return hdfsSourceEntityMgr.getVersions(ingestion);
        } catch (Exception ex) {
            log.error("Fail to get all versions for ingestion " + strategy.getSource(), ex);
        }
        return null;
    }

    @Override
    protected Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        List<String> hiveTables = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = hdfsPathBuilder.constructIngestionDir(strategy.getSource(), version).toString();
            hdfsPaths.add(hdfsPath);
        });

        return Pair.of(hdfsPaths, hiveTables);
    }

    @Override
    public boolean isSourceExisted(PurgeStrategy strategy) {
        Source source = new IngestionSource();
        ((IngestionSource) source).setIngestionName(strategy.getSource());
        return hdfsSourceEntityMgr.checkSourceExist(source);
    }

}
