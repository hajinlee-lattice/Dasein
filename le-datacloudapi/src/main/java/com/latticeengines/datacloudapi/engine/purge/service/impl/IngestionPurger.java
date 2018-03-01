package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("ingestionPurger")
public class IngestionPurger extends ConfigurablePurger{

    @Override
    public boolean isToBak() {
        return true;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.INGESTION_SOURCE;
    }

    @Override
    public Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = hdfsPathBuilder.constructIngestionDir(strategy.getSource(), version).toString();
            hdfsPaths.add(hdfsPath);
        });
        return Pair.of(hdfsPaths, null);
    }

    @Override
    public List<String> findAllVersions(PurgeStrategy strategy) {
        IngestionSource ingestion = new IngestionSource();
        ingestion.setIngestionName(strategy.getSource());
        return hdfsSourceEntityMgr.getVersions(ingestion);
    }

}
