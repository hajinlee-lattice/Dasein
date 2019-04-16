package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MergeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeAccount.class);

    static final String BEAN_NAME = "mergeAccount";

    private int mergeStep;
    private int upsertMasterStep;
    private int diffStep;

    private String diffTableNameInContext;
    private String batchStoreNameInContext;

    private boolean shortCutMode;

    private String matchedTableFromContact;

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

    private boolean isShortCutMode() {
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

        boolean entityMatchEnabled = configuration.isEntityMatchEnabled();
        if (!entityMatchEnabled) {
            upsertMasterStep = 0;
            diffStep = 1;
            String matchedTable = getMatchedTable();
            TransformationStepConfig upsertMaster = mergeMaster(entityMatchEnabled, matchedTable);
            TransformationStepConfig diff = diff(matchedTable, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);
            steps.add(upsertMaster);
            steps.add(diff);
            steps.add(report);
        } else {
            mergeStep = 0;
            upsertMasterStep = 1;
            diffStep = 2;
            List<String> matchedTables = getAllMatchedTable();
            TransformationStepConfig merge = mergeInputs(false, false, true, false, null, null,
                    InterfaceName.EntityId.name(), matchedTables);
            TransformationStepConfig upsertMaster = mergeMaster(entityMatchEnabled, mergeStep);
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);
            steps.add(merge);
            steps.add(upsertMaster);
            steps.add(diff);
            steps.add(report);
        }

        return steps;
    }

    private List<String> getAllMatchedTable() {
        List<String> matchedTables = new ArrayList<>();
        String matchedTable = getMatchedTable();
        if (StringUtils.isNotBlank(matchedTable)) {
            matchedTables.add(matchedTable);
        }
        if (StringUtils.isNotBlank(matchedTableFromContact)) {
            matchedTables.add(matchedTableFromContact);
        }
        return matchedTables;
    }

    private String getMatchedTable() {
        return getStringValueFromContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE);
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
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        matchedTableFromContact = getStringValueFromContext(ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE);
        if (StringUtils.isNotBlank(matchedTableFromContact)) {
            inputTableNames.add(matchedTableFromContact);
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();

        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        exportToS3AndAddToContext(batchStoreTableName, ACCOUNT_MASTER_TABLE_NAME);
        exportToS3AndAddToContext(diffTableName, ACCOUNT_DIFF_TABLE_NAME);
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
