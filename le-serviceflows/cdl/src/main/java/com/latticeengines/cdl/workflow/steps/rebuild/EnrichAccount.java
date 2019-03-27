package com.latticeengines.cdl.workflow.steps.rebuild;


import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(EnrichAccount.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EnrichAccount extends ProfileStepBase<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "enrichAccount";

    private static final Logger log = LoggerFactory.getLogger(EnrichAccount.class);

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private CloneTableService cloneTableService;

    private String fullAccountTablePrefix = "FullAccount";
    private String masterTableName;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        customerSpace = configuration.getCustomerSpace();
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
        masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                inactive);
        if (StringUtils.isBlank(masterTableName)) {
            masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                    active);
            if (StringUtils.isNotBlank(masterTableName)) {
                log.info("Found the batch store in active version " + active + ": " + masterTableName);
                cloneTableService.setActiveVersion(active);
                cloneTableService.setCustomerSpace(customerSpace);
                cloneTableService.linkInactiveTable(batchStore);
            }
        } else {
            log.info("Found the batch store in inactive version " + inactive + ": " + masterTableName);
        }

        String fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
        if (StringUtils.isNotBlank(fullAccountTableName)) {
            Table fullAccountTable = metadataProxy.getTable(customerSpace.toString(), fullAccountTableName);
            if (fullAccountTable != null) {
                log.info("Found full account table in context, go thru short-cut mode.");
                addToListInContext(TEMPORARY_CDL_TABLES, fullAccountTableName, String.class);
                return null;
            }
        }

        if (StringUtils.isBlank(masterTableName)) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
        Table masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
        long count = ScalingUtils.getTableCount(masterTable);
        int multiplier = ScalingUtils.getMultiplier(count);
        if (multiplier > 1) {
            log.info("Set multiplier=" + multiplier + " base on master table count=" + count);
            scalingMultiplier = multiplier;
        }

        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String fullAccountTableName = TableUtils.getFullTableName(fullAccountTablePrefix, pipelineVersion);
        exportToS3AndAddToContext(fullAccountTableName, FULL_ACCOUNT_TABLE_NAME);
        addToListInContext(TEMPORARY_CDL_TABLES, fullAccountTableName, String.class);
    }

    private PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("EnrichAccount");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);

        TransformationStepConfig match = match(customerSpace, masterTableName);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(match);
        // There was an old step to merge fetched result with slim batch store
        // Which can be found in branch-4.14
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig match(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);

        setBaseTables(sourceTableName, step);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(fullAccountTablePrefix);
        step.setTargetTable(targetTable);

        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));

        String dataCloudVersion = "";
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (detail != null) {
            try {
                dataCloudVersion = DataCloudVersion.parseBuildNumber(detail.getDataCloudBuildNumber()).getVersion();
            } catch (Exception e) {
                log.warn("Failed to read datacloud version from collection status " + JsonUtils.serialize(detail));
            }
        }

        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns(dataCloudVersion);
        List<Column> cols = new ArrayList<>();
        for (ColumnMetadata cm : dcCols) {
            cols.add(new Column(cm.getAttrName()));
        }
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(cols);

        matchInput.setCustomSelection(cs);
        matchInput.setUnionSelection(null);
        matchInput.setPredefinedSelection(null);
        matchInput.setKeyMap(getKeyMap());
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setFetchOnly(true);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        config.setMatchInput(matchInput);
        step.setConfiguration(JsonUtils.serialize(config));

        return step;
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        return keyMap;
    }

}
