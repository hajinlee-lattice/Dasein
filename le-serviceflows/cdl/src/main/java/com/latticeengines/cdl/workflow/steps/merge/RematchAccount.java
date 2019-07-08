package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.util.TableUtils;

/**
 * Should only run this step for non-EntityMatch tenants
 */
@Component(RematchAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RematchAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RematchAccount.class);

    static final String BEAN_NAME = "rematchAccount";

    private String masterTableName;
    private String rematchedTableName;

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        setMasterTableName();

        String rematchedTableNameInContext = getStringValueFromContext(REMATCHED_ACCOUNT_TABLE_NAME);
        if (StringUtils.isNotBlank(rematchedTableNameInContext)) {
            Table rematchedTable = metadataProxy.getTable(customerSpace.toString(), rematchedTableNameInContext);
            if (rematchedTable != null) {
                log.info("Found re-matched table in context, going through short-cut mode.");
                rematchedTableName = rematchedTableNameInContext;
                registerBatchStore();
                if (!rematchedTable.getName().equalsIgnoreCase(masterTableName)) {
                    addToListInContext(TEMPORARY_CDL_TABLES, masterTableName, String.class);
                }
                return null;
            }
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("RematchAccount");
        int dropStep = 0;
        List<TransformationStepConfig> steps = new ArrayList<>();
        TransformationStepConfig drop = dropRefreshingIds();
        TransformationStepConfig match = match(dropStep);
        steps.add(drop);
        steps.add(match);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        registerBatchStore();
        putStringValueInContext(FULL_ACCOUNT_TABLE_NAME, getBatchStoreName());
        addToListInContext(TEMPORARY_CDL_TABLES, masterTableName, String.class);
    }

    private TransformationStepConfig dropRefreshingIds() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_COPY_TXMFR);
        addBaseTables(step, masterTableName);
        CopyConfig config = new CopyConfig();
        List<String> colsToDrop = new ArrayList<>();
        colsToDrop.add(InterfaceName.LatticeAccountId.name());
        config.setDropAttrs(colsToDrop);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig match(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setConfiguration(getMatchConfig());
        setTargetTable(step, batchStoreTablePrefix);
        return step;
    }

    private String getMatchConfig() {
        MatchInput matchInput = getBaseMatchInput();
        Set<String> columnNames = getMasterTableColumnNames();
        return MatchUtils.getLegacyMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames);
    }

    private void setMasterTableName() {
        TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
        masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                inactive);
        if (StringUtils.isBlank(masterTableName)) {
            masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                    active);
            if (StringUtils.isNotBlank(masterTableName)) {
                log.info("Found the batch store in active version " + active + ": " + masterTableName);
            }
        } else {
            log.info("Found the batch store in inactive version " + inactive + ": " + masterTableName);
        }
        if (StringUtils.isBlank(masterTableName)) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
    }

    private Set<String> getMasterTableColumnNames() {
        Table masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
        return new HashSet<>(Arrays.asList(masterTable.getAttributeNames()));
    }

    @Override
    protected String getBatchStoreName() {
        if (StringUtils.isNotBlank(rematchedTableName)) {
            return rematchedTableName;
        } else {
            return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
        }
    }

}
