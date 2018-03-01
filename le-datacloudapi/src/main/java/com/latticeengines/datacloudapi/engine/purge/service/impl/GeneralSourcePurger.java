package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("generalSourcePurger")
public class GeneralSourcePurger extends ConfigurablePurger {
    @Override
    public boolean isToBak() {
        return true;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.GENERAL_SOURCE;
    }

    @Override
    public List<String> findAllVersions(PurgeStrategy strategy) {
        return hdfsSourceEntityMgr.getVersions(new GeneralSource(strategy.getSource()));
    }

    @Override
    public Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        List<String> hiveTables = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = hdfsPathBuilder.constructSnapshotDir(strategy.getSource(), version).toString();
            hdfsPaths.add(hdfsPath);
            String hiveTable = hiveTableService.tableName(strategy.getSource(), version);
            hiveTables.add(hiveTable);
        });

        return Pair.of(hdfsPaths, hiveTables);
    }
}
