package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;

public abstract class AMSourcePurger extends ConfigurablePurger {

    private static Logger log = LoggerFactory.getLogger(AMSourcePurger.class);

    protected abstract String getHdfsVersionFromDCVersion(DataCloudVersion dcVersion);

    /*
     * Delete versions which are not used in prod and were created 90 days ago
     */
    @Override
    protected List<String> findVersionsToDelete(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        List<DataCloudVersion> dcVersions = dataCloudVersionEntityMgr.allVerions();
        Set<String> approvedVersions = new HashSet<>();
        dcVersions.forEach(dcv -> {
            approvedVersions.add(getHdfsVersionFromDCVersion(dcv));
        });
        List<String> toDelete = new ArrayList<>();
        for (String version : currentVersions) {
            if (approvedVersions.contains(version)) {
                continue;
            }
            if (debug) {
                toDelete.add(version);
                continue;
            }
            String hdfsPath = hdfsPathBuilder.constructSnapshotDir(strategy.getSource(), version).toString();
            try {
                FileStatus status = HdfsUtils.getFileStatus(yarnConfiguration, hdfsPath);
                if (System.currentTimeMillis() - status.getModificationTime() > 90 * DAY_IN_MS) {
                    toDelete.add(version);
                }
            } catch (IOException e) {
                log.error("Fail to get file status for hdfs path " + hdfsPath, e);
            }
        }
        return toDelete;
    }

    /*
     * Backup and then delete old versions which are used in PROD
     */
    protected List<String> findVersionsToBak(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        if (strategy.getHdfsVersions() <= 0) {
            throw new RuntimeException(
                    "HDFS version for source " + strategy.getSource() + " is set as 0 or invalid");
        }

        List<DataCloudVersion> dcVersions = dataCloudVersionEntityMgr.allVerions();
        String latestDCVersion = dataCloudVersionEntityMgr.currentApprovedVersionAsString();
        List<String> recentDCVersions = dataCloudVersionService.priorVersions(latestDCVersion,
                strategy.getHdfsVersions());
        Set<String> recentDCVersionSet = new HashSet<>(recentDCVersions);
        Set<String> approvedVersions = new HashSet<>();
        dcVersions.forEach(dcv -> {
            if (!recentDCVersionSet.contains(dcv.getVersion())) {
                approvedVersions.add(getHdfsVersionFromDCVersion(dcv));
            }
        });

        List<String> toBak = new ArrayList<>();
        for (String version : currentVersions) {
            if (approvedVersions.contains(version)) {
                toBak.add(version);
            }
        }
        return toBak;
    }

}
