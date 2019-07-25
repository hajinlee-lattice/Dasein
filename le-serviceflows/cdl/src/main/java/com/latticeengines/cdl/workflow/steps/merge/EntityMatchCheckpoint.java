package com.latticeengines.cdl.workflow.steps.merge;

import java.util.Arrays;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.HdfsS3ImporterExporter;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.EMREnvService;

/**
 * Run this step after all entity match steps.
 * If passed this step, in retry, skip all entity match steps.
 */
@Component("entityMatchCheckpoint")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EntityMatchCheckpoint extends BaseWorkflowStep<ProcessStepConfiguration> {

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Resource(name = "distCpConfiguration")
    protected Configuration distCpConfiguration;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    protected CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        boolean isCompleted = Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
        if (!isCompleted) {
            for (String contextKey : Arrays.asList( //
                    ENTITY_MATCH_ACCOUNT_TARGETTABLE, //
                    ENTITY_MATCH_CONTACT_TARGETTABLE, //
                    ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE //
            )) {
                exportToS3AndAddToTempList(contextKey);
            }
            putObjectInContext(ENTITY_MATCH_COMPLETED, true);
        }
    }

    private void exportToS3AndAddToTempList(String contextKey) {
        String tableName = getStringValueFromContext(contextKey);
        if (StringUtils.isBlank(tableName)) {
            log.warn("Cannot find table " + contextKey + " in workflow context");
            return;
        }

        boolean shouldSkip = getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class);
        if (shouldSkip) {
            log.info("Skip publish " + contextKey + " (" + tableName + ") to S3.");
        } else {
            HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
            String queueName = LedpQueueAssigner.getEaiQueueNameForSubmission();
            queueName = LedpQueueAssigner.overwriteQueueAssignment(queueName, emrEnvService.getYarnQueueScheme());
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            ImportExportRequest batchStoreRequest = ImportExportRequest.exportAtlasTable( //
                    customerSpace.toString(), table, //
                    pathBuilder, s3Bucket, podId, //
                    yarnConfiguration, //
                    fileStatus -> true);
            if (batchStoreRequest == null) {
                throw new IllegalArgumentException("Cannot construct proper export request for " + tableName);
            }
            HdfsS3ImporterExporter exporter = new HdfsS3ImporterExporter( //
                    customerSpace.toString(), distCpConfiguration, queueName, dataUnitProxy, batchStoreRequest);
            exporter.run();
        }
        addToListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);
    }

}
