package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
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

    private int mergeStep;
    private int matchStep;
    private int fetchOnlyMatchStep;
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

        diffTableNameInContext = getStringValueFromContext(ACCOUNT_DIFF_TABLE_NAME);
        batchStoreNameInContext = getStringValueFromContext(ACCOUNT_MASTER_TABLE_NAME);
        Table diffTableInContext = StringUtils.isNotBlank(diffTableNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), diffTableNameInContext) : null;
        Table batchStoreInContext = StringUtils.isNotBlank(batchStoreNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), batchStoreNameInContext) : null;
        if (diffTableInContext != null && batchStoreInContext != null) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            shortCutMode = true;
            diffTableName = diffTableNameInContext;
            request.setSteps(shortCutSteps());
        } else {
            request.setSteps(regularSteps());
        }

        return request;
    }

    private List<TransformationStepConfig> regularSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();

        mergeStep = 0;
        matchStep = 1;
        TransformationStepConfig merge = mergeInputs(false, true, false);
        TransformationStepConfig match = match(mergeStep);
        steps.add(merge);
        steps.add(match);

        if (configuration.isEntityMatchEnabled()) {
            fetchOnlyMatchStep = 2;
            upsertMasterStep = 3;
            diffStep = 4;
            TransformationStepConfig fetchOnlyMatch = fetchOnlyMatch(matchStep);
            TransformationStepConfig upsertMaster = mergeMaster(fetchOnlyMatchStep);
            steps.add(fetchOnlyMatch);
            steps.add(upsertMaster);
        } else {
            upsertMasterStep = 2;
            diffStep = 3;
            TransformationStepConfig upsertMaster = mergeMaster(fetchOnlyMatchStep);
            steps.add(upsertMaster);
        }

        TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
        TransformationStepConfig report = reportDiff(diffStep);
        steps.add(diff);
        steps.add(report);

        return steps;
    }

    private List<TransformationStepConfig> shortCutSteps() {
        TransformationStepConfig report = reportDiff(diffTableName);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(report);
        return steps;
    }

    private TransformationStepConfig match(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig());
        return step;
    }

    private TransformationStepConfig fetchOnlyMatch(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getFetchOnlyMatchConfig());
        return step;
    }

    @Override
    protected TransformationStepConfig mergeMaster(int matchStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step);
        step.setInputSteps(Collections.singletonList(matchStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(getMergeMasterConfig());
        setTargetTable(step, batchStoreTablePrefix);
        return step;
    }

    private String getMergeMasterConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name());
        config.setColumnsFromRight(Collections.singleton(InterfaceName.CDLCreatedTime.name()));
        return appendEngineConf(config, heavyEngineConfig());
    }

    private String getMatchConfig() {
        MatchInput matchInput = getBaseMatchInput();
        Set<String> columnNames = getInputTableColumnNames(0);
        if (configuration.isEntityMatchEnabled()) {
            return MatchUtils.getAllocateIdMatchConfigForAccount(matchInput, columnNames);
        } else {
            return MatchUtils.getLegacyMatchConfigForAccount(matchInput, columnNames);
        }
    }

    private String getFetchOnlyMatchConfig() {
        return MatchUtils.getFetchOnlyMatchConfigForAccount(getBaseMatchInput());
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

    protected String getDiffTableName() {
        if (shortCutMode) {
            return diffTableNameInContext;
        } else {
            return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        }
    }

}
