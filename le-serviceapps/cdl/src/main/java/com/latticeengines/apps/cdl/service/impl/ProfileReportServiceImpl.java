package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.FULL_PROFILE;
import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status.READY;
import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status.STALE;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ProfileReportService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;


@Service
public class ProfileReportServiceImpl implements ProfileReportService {

    @Inject
    private DataCollectionService dataCollectionService;

    @Override
    public AtlasProfileReportStatus getStatus() {
        AtlasProfileReportStatus status = new AtlasProfileReportStatus();

        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version activeVersion = dataCollectionService.getActiveVersion(customerSpace);
        DataCollectionArtifact activeArtifact = //
                dataCollectionService.getLatestArtifact(customerSpace, FULL_PROFILE, activeVersion);
        DataCollection.Version inactiveVersion = activeVersion.complement();
        DataCollectionArtifact inactiveArtifact = //
                dataCollectionService.getLatestArtifact(customerSpace, FULL_PROFILE, inactiveVersion);

        Long lastRefreshTime = null;
        DataCollectionArtifact artifact = null;
        if (activeArtifact == null && inactiveArtifact == null) {
            status.setStatus(AtlasProfileReportStatus.Status.Never);
        } else if (activeArtifact == null) {
            artifact = inactiveArtifact;
            lastRefreshTime = artifact.getCreateTime();
        } else {
            artifact = activeArtifact;
            lastRefreshTime = artifact.getCreateTime();
            if (inactiveArtifact != null) {
                lastRefreshTime = Math.max(inactiveArtifact.getCreateTime(), lastRefreshTime);
            }
        }

        if (artifact != null) {
            if (!artifact.getStatus().isTerminal()) {
                status.setStatus(AtlasProfileReportStatus.Status.Generating);
                status.setLastRefreshTime(lastRefreshTime);
            } else if (READY.equals(artifact.getStatus()) || STALE.equals(artifact.getStatus())) {
                status.setStatus(AtlasProfileReportStatus.Status.Ready);
                status.setLastRefreshTime(artifact.getCreateTime());
            }
        }

        return status;
    }

}
