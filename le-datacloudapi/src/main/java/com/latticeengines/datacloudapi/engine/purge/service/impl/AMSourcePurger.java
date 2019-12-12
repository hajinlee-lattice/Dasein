package com.latticeengines.datacloudapi.engine.purge.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER_LOOKUP;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.DUNS_GUIDE_BOOK;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * AccountMaster related sources:
 * AccountMaster, AccountMasterLookup & DunsGuideBook
 *
 * Versions expected to be left on HDFS:
 *      most recent {@link #PurgeStrategy.getHdfsVersions}} versions which are approved in DataCloudVersion table
 *    + versions with timestamp newer than most recent approved version
 *
 */
@Component("amSourcePurger")
public class AMSourcePurger extends VersionedPurger {

    @Override
    protected SourceType getSourceType() {
        return SourceType.AM_SOURCE;
    }

    @Override
    protected List<String> findPurgeVersions(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        if (strategy.getHdfsVersions() == null) {
            throw new IllegalArgumentException(
                    "Must provide HdfsVersions in PurgeStrategy for source in type AM_SOURCE");
        }
        List<DataCloudVersion> dcVersions = dataCloudVersionEntityMgr.allApprovedVerions();
        if (CollectionUtils.isEmpty(dcVersions)) {
            return null;
        }
        Collections.sort(dcVersions, DataCloudVersion.versionComparator);
        Set<String> retainedApprovedVersions = new HashSet<>();
        int nRetainedVersions = Math.min(dcVersions.size(), strategy.getHdfsVersions());
        for (int i = 0; i < nRetainedVersions; i++) {
            String hdfsVersion = getHdfsVersionFromDCVersion(strategy.getSource(),
                    dcVersions.get(dcVersions.size() - 1 - i));
            // For old DataCloudVersion, it might not have HdfsVersion populated
            // for DunsGuideBook due to DunsGuideBook was introduced late.
            if (hdfsVersion != null) {
                retainedApprovedVersions.add(hdfsVersion);
            }
        }
        // Latest DataCloudVersion should have HdfsVersion populated for all the
        // sources: AccountMaster, AccountMasterLookup & DunsGuideBook
        String latestApprovedVersion = getHdfsVersionFromDCVersion(strategy.getSource(),
                dcVersions.get(dcVersions.size() - 1));

        Iterator<String> iter = allVersions.iterator();
        while (iter.hasNext()) {
            String version = iter.next();
            if (retainedApprovedVersions.contains(version) || version.compareTo(latestApprovedVersion) >= 0) {
                iter.remove();
            }
        }

        return allVersions;
    }

    private String getHdfsVersionFromDCVersion(String srcName, DataCloudVersion dcv) {
        if (ACCOUNT_MASTER.equals(srcName)) {
            return dcv.getAccountMasterHdfsVersion();
        }
        if (ACCOUNT_MASTER_LOOKUP.equals(srcName)) {
            return dcv.getAccountLookupHdfsVersion();
        }
        if (DUNS_GUIDE_BOOK.equals(srcName)) {
            return dcv.getDunsGuideBookHdfsVersion();
        }
        throw new RuntimeException(
                "SourceType AM_SOURCE only support source AccountMaster, AccountMasterLookup & DunsGuideBook");
    }

}
