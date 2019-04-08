package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.HdfsS3ImporterExporter;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component(MergeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeAccount.class);

    static final String BEAN_NAME = "mergeAccount";

    private int upsertMasterStep;
    private int diffStep;

    private String diffTableNameInContext;
    private String batchStoreNameInContext;

    private boolean shortCutMode;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Resource(name = "distCpConfiguration")
    protected Configuration distCpConfiguration;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeAccount");

        if (isShortCutMode()) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            shortCutMode = true;
            diffTableName = diffTableNameInContext;
            request.setSteps(shortCutSteps());
        } else {
            request.setSteps(regularSteps());
        }

        return request;
    }

    protected boolean isShortCutMode() {
        diffTableNameInContext = getStringValueFromContext(ACCOUNT_DIFF_TABLE_NAME);
        batchStoreNameInContext = getStringValueFromContext(ACCOUNT_MASTER_TABLE_NAME);
        Table diffTableInContext = StringUtils.isNotBlank(diffTableNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), diffTableNameInContext) : null;
        Table batchStoreInContext = StringUtils.isNotBlank(batchStoreNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), batchStoreNameInContext) : null;
        return diffTableInContext != null && batchStoreInContext != null;
    }

    private List<TransformationStepConfig> regularSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();

        upsertMasterStep = 0;
        diffStep = 1;
        String matchedTable = getMatchedTable();
        TransformationStepConfig upsertMaster = mergeMaster(matchedTable, configuration.isEntityMatchEnabled());
        TransformationStepConfig diff = diff(matchedTable, upsertMasterStep);
        TransformationStepConfig report = reportDiff(diffStep);
        steps.add(upsertMaster);
        steps.add(diff);
        steps.add(report);

        return steps;
    }

    private String getMatchedTable() {
        String matchedTable = getStringValueFromContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE);
        if (StringUtils.isBlank(matchedTable)) {
            throw new RuntimeException("There's no matched table found!");
        }
        return matchedTable;
    }

    private List<TransformationStepConfig> shortCutSteps() {
        TransformationStepConfig report = reportDiff(diffTableName);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(report);
        return steps;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            attr0.setTags(Tag.INTERNAL);
            attrs.add(attr0);
        });
        table.setAttributes(attrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();

        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        exportToS3AndAddToContext(batchStoreTableName, ACCOUNT_MASTER_TABLE_NAME);
        exportToS3AndAddToContext(diffTableName, ACCOUNT_DIFF_TABLE_NAME);
    }

    private void exportToS3AndAddToContext(String tableName, String contextKey) {
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
        putStringValueInContext(contextKey, tableName);
    }

    @Override
    protected String getBatchStoreName() {
        if (shortCutMode) {
            return batchStoreNameInContext;
        } else {
            return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
        }
    }

    @Override
    protected String getDiffTableName() {
        if (shortCutMode) {
            return diffTableNameInContext;
        } else {
            return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        }
    }

}
