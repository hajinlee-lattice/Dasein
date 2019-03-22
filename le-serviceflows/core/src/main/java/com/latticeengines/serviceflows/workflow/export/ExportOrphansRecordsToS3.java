package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportOrphansToS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportOrphanRecordsToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportOrphansRecordsToS3 extends BaseImportExportS3<ExportOrphansToS3StepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ExportOrphansRecordsToS3.class);
    private static final String PATH_SEPARATOR = "/";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private String exportPath;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        String orphanFileName = getStringValueFromContext(MERGED_FILE_NAME);

        ImportExportRequest request = new ImportExportRequest();
        request.srcPath = configuration.getSourcePath() + "/" + orphanFileName;
        String atlasFilePath = pathBuilder.convertAtlasFile(request.srcPath, podId, tenantId, s3Bucket);
        int lastSlashIndex = atlasFilePath.lastIndexOf(PATH_SEPARATOR);
        exportPath = StringUtils.join(new String[] {
                atlasFilePath.substring(0, lastSlashIndex),
                configuration.getExportId(),
                atlasFilePath.substring(lastSlashIndex + 1)
        }, PATH_SEPARATOR);
        request.tgtPath = exportPath;
        log.info("srcPath=" + request.srcPath);
        log.info("tgtPath=" + request.tgtPath);
        requests.add(request);
    }

    @Override
    public void onExecutionCompleted() {
        String customerSpace = configuration.getCustomerSpace().toString();
        OrphanRecordsType orphanType = configuration.getOrphanRecordsType();
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace);

        // update data collection status
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, version);
        Long orphanCount = getLongValueFromContext(ORPHAN_COUNT);
        switch (orphanType) {
            case CONTACT:
                status.setOrphanContactCount(orphanCount);
                break;
            case TRANSACTION:
                status.setOrphanTransactionCount(orphanCount);
                break;
            case UNMATCHED_ACCOUNT:
                status.setUnmatchedAccountCount(orphanCount);
                break;
            default:
                throw new IllegalArgumentException("Unknown orphan records type: " + orphanType.name());
        }
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace, status, version);
        log.info(String.format("Updated orphan records count=%d of type %s", orphanCount, orphanType.getOrphanType()));

        // update or create data collection artifact
        DataCollectionArtifact artifact = dataCollectionProxy.getDataCollectionArtifact(
                customerSpace, orphanType.getOrphanType(), version);
        artifact.setUrl(exportPath);
        artifact.setStatus(DataCollectionArtifact.Status.READY);
        artifact = dataCollectionProxy.updateDataCollectionArtifact(customerSpace, artifact);
        log.info(String.format("Updated dataCollectionArtifact of pid=%s, name=%s, url=%s, status=%s",
                artifact.getPid(), artifact.getName(), artifact.getUrl(), artifact.getStatus()));
    }
}
