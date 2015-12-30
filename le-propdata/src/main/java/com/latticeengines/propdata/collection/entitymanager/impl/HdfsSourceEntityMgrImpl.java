package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.collection.entitymanager.HdfsSourceEntityMgr;
import com.latticeengines.propdata.collection.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.ServingSource;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.propdata.collection.util.TableUtils;

@Component("hdfsSourceEntityMgr")
public class HdfsSourceEntityMgrImpl implements HdfsSourceEntityMgr {

    @Autowired
    HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    YarnConfiguration yarnConfiguration;

    @Override
    public String getCurrentVersion(Source source) {
        String versionFile = hdfsPathBuilder.constructVersionFile(source).toString();
        int retries = 0;
        while (retries++ < 3) {
            try {
                return HdfsUtils.getHdfsFileContents(yarnConfiguration, versionFile);
            } catch (Exception e) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
        }
        throw new RuntimeException("Could not determine the current version of source " + source.getSourceName());
    }

    @Override
    public synchronized void setCurrentVersion(Source source, String version) {
        String versionFile = hdfsPathBuilder.constructVersionFile(source).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, versionFile)) {
                HdfsUtils.rmdir(yarnConfiguration, versionFile);
            }
            HdfsUtils.writeToFile(yarnConfiguration, versionFile, version);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getCurrentSnapshotDir(Source source) {
        String version = getCurrentVersion(source);
        return hdfsPathBuilder.constructSnapshotDir(source, version).toString();
    }

    @Override
    public Table getTableAtVersion(Source source, String version) {
        if (source instanceof ServingSource) {
            String path = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
            return TableUtils.createTable(((ServingSource) source).getSqlTableName(), path + "/*.avro");
        } else if (source instanceof BulkSource ) {
            String path = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
            return TableUtils.createTable(source.getSourceName(), path + "/*.avro");
        } else if (source instanceof CollectedSource) {
            String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
            List<String> avroPaths = new ArrayList<>();
            try {
                for (String dir : HdfsUtils.getFilesForDir(yarnConfiguration, rawDir)) {
                    if (HdfsUtils.isDirectory(yarnConfiguration, dir)) {
                        dir = dir.substring(dir.lastIndexOf("/") + 1);
                        avroPaths.add(rawDir + "/" + dir + "/*.avro");
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to get all incremental raw data dirs for " + source.getSourceName());
            }
            return TableUtils.createTable(source.getSourceName(),
                    avroPaths.toArray(new String[avroPaths.size()]), source.getPrimaryKey());
        } else {
            throw new UnsupportedOperationException("Do not know how to extract table for the given source type.");
        }
    }

}
