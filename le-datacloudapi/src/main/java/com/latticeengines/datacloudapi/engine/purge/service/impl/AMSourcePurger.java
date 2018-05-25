package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * Left in HDFS: 
 *      most recent 2 versions (strategy.hdfsVersions) which are approved in DataCloudVersion table
 *    + all the new versions after most recent approved version
 * To back up: 
 *      versions older than most recent 2 which are approved in DataCloudVersion table
 * To delete: 
 *      others
 *
 */
@Component("amSourcePurger")
public class AMSourcePurger extends VersionedPurger {

    public static final String ACCOUNT_MASTER = "AccountMaster";
    public static final String ACCOUNT_MASTER_LOOKUP = "AccountMasterLookup";

    @Override
    protected SourceType getSourceType() {
        return SourceType.AM_SOURCE;
    }

    @Override
    protected List<String> findVersionsToDelete(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        List<DataCloudVersion> dcVersions = dataCloudVersionEntityMgr.allApprovedVerions();
        if (CollectionUtils.isEmpty(dcVersions)) {
            return null;
        }
        DataCloudVersion latestApprovedDCVersion = Collections.max(dcVersions,
                Comparator.comparing(dc -> dc.getVersion()));
        String latestApprovedVersion = getHdfsVersionFromDCVersion(strategy.getSource(), latestApprovedDCVersion);
        Set<String> approvedVersions = new HashSet<>();
        dcVersions.forEach(dcv -> {
            approvedVersions.add(getHdfsVersionFromDCVersion(strategy.getSource(), dcv));
        });

        List<String> toDelete = new ArrayList<>();
        for (String version : allVersions) {
            if (!approvedVersions.contains(version) && version.compareTo(latestApprovedVersion) < 0) {
                toDelete.add(version);
            }
        }

        return toDelete;
    }

    protected List<String> findVersionsToBak(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        List<DataCloudVersion> dcVersions = dataCloudVersionEntityMgr.allApprovedVerions();
        Collections.sort(dcVersions, (dc1, dc2) -> dc2.getVersion().compareToIgnoreCase(dc1.getVersion())); // Descend
        String versionToCompare = dcVersions.size() >= strategy.getHdfsVersions()
                ? getHdfsVersionFromDCVersion(strategy.getSource(), dcVersions.get(strategy.getHdfsVersions() - 1))
                : getHdfsVersionFromDCVersion(strategy.getSource(), dcVersions.get(dcVersions.size() - 1));
        Set<String> approvedVersions = new HashSet<>();
        dcVersions.forEach(dcv -> {
            approvedVersions.add(getHdfsVersionFromDCVersion(strategy.getSource(), dcv));
        });

        List<String> toDelete = new ArrayList<>();
        for (String version : allVersions) {
            if (approvedVersions.contains(version) && version.compareTo(versionToCompare) < 0) {
                toDelete.add(version);
            }
        }

        return toDelete;
    }

    private String getHdfsVersionFromDCVersion(String srcName, DataCloudVersion dcv) {
        if (ACCOUNT_MASTER.equals(srcName)) {
            return dcv.getAccountMasterHdfsVersion();
        }
        if (ACCOUNT_MASTER_LOOKUP.equals(srcName)) {
            return dcv.getAccountLookupHdfsVersion();
        }
        throw new RuntimeException("SourceType AM_SOURCE only support source AccountMaster and AccountMasterLookup");
    }

}
