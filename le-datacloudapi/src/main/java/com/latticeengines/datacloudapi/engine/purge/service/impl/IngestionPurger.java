package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("ingestionPurger")
public class IngestionPurger extends ConfigurablePurger{

    @Autowired
    HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Override
    public boolean isToBak() {
        return true;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.INGESTION_SOURCE;
    }

    @Override
    public Pair<List<String>, List<String>> getHdfsPathsHiveTablesToPurge(PurgeStrategy strategy) {
        if (strategy.getHdfsVersions() == 0) {
            throw new RuntimeException(
                    "HDFS version for source " + strategy.getSource() + " is set as 0. Too dangerous");
        }
        List<String> versionsToPurge = findVersionsToPurge(strategy);
        if (CollectionUtils.isEmpty(versionsToPurge)) {
            return null;
        }
        return constructHdfsPathsHiveTables(strategy, versionsToPurge);
    }

    private List<String> findVersionsToPurge(PurgeStrategy strategy) {
        IngestionSource ingestion = new IngestionSource();
        ingestion.setIngestionName(strategy.getSource());
        List<String> versionsToPurge = hdfsSourceEntityMgr.getVersions(ingestion);
        if (CollectionUtils.isEmpty(versionsToPurge)) {
            return null;
        }
        Collections.sort(versionsToPurge);
        if (versionsToPurge.size() <= strategy.getHdfsVersions()) {
            return null;
        }
        for (int i = 0; i < strategy.getHdfsVersions(); i++) {
            versionsToPurge.remove(versionsToPurge.size() - 1); // Retain latest
                                                                // versions
        }
        return versionsToPurge;
    }

    private Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = hdfsPathBuilder.constructIngestionDir(strategy.getSource(), version).toString();
            hdfsPaths.add(hdfsPath);
        });
        return Pair.of(hdfsPaths, null);
    }

}
