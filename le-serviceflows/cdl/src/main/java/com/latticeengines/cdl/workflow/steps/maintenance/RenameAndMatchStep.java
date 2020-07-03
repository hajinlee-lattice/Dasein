package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY_TXFMR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.cdl.workflow.steps.merge.MatchUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.RenameAndMatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("renameAndMatchStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RenameAndMatchStep extends BaseTransformWrapperStep<RenameAndMatchStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(RenameAndMatchStep.class);

    static final String BEAN_NAME = "renameAndMatchStep";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private ActionProxy actionProxy;

    private Table masterTable;

    private BusinessEntity idEntity;

    private EntityType deleteEntityType;

    private String idSystem;

    private List<String> sourceTableColumns;

    private String systemIdColumn;

    private String renameAndMatchTablePrefix = null;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();

        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request,
                configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String outputTableName = getOutputTableName();
        // Validate the output to make sure the idEntity column has values,
        // otherwise throw exception
        Table outputTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), outputTableName);
        String path = outputTable.getExtracts().get(0).getPath();
        log.info("RenameAndMatchStep, output table name {}, path is {}", outputTableName, path);
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, path);
        int cnt = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String columnForDelete = idEntity.equals(BusinessEntity.Account) ? InterfaceName.AccountId.name()
                    : InterfaceName.ContactId.name();
            if (record.get(columnForDelete) != null) {
                String value = record.get(columnForDelete).toString();
                if (!StringUtils.isEmpty(value)) {
                    cnt++;
                }
            }
        }
        if (cnt == 0) {
            throw new RuntimeException("No ID is found after match, can't proceed. Please check the input!");
        }

        // Save output table for downstream steps
        saveOutputValue(WorkflowContextConstants.Outputs.RENAME_AND_MATCH_TABLE, outputTableName);

        // Update Action with new DeleteDataTable
        String actionPid = configuration.getDeleteActionPid();
        if (actionPid != null) {
            Long pid = Long.parseLong(actionPid);
            log.info("RenameAndMatchStep, action pid {}", pid);
            Action action = actionProxy
                    .getActionsByPids(configuration.getCustomerSpace().toString(), Collections.singletonList(pid))
                    .get(0);
            if (action != null) {
                DeleteActionConfiguration actionConfiguration = (DeleteActionConfiguration) action
                        .getActionConfiguration();
                actionConfiguration.setDeleteDataTable(outputTableName);
                action.setActionConfiguration(actionConfiguration);
                actionProxy.updateAction(configuration.getCustomerSpace().toString(), action);
            } else {
                log.warn(String.format("Action with pid=%d cannot be found", pid));
            }
        } else {
            log.warn("ActionPid is not passed in");
        }
    }

    protected void intializeConfiguration() {
        idEntity = configuration.getIdEntity();
        if (!BusinessEntity.Account.equals(idEntity) && !BusinessEntity.Contact.equals(idEntity)) {
            throw new RuntimeException("RenameAndMatchStep, idEntity should be either Account or Contact");
        }

        deleteEntityType = configuration.getDeleteEntityType();
        idSystem = configuration.getIdSystem();
        systemIdColumn = getSystemIdColumn(idEntity, idSystem);
        log.info("RenameAndMatchStep, systemIdColumn is " + systemIdColumn.toString());

        Table sourceTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTableName());
        sourceTableColumns = sourceTable.getAttributes().stream().map(attr -> attr.toString())
                .collect(Collectors.toList());
        log.info("RenameAndMatchStep, source table {}, table columns {} ", sourceTable.getName(), sourceTableColumns);
    }

    private PipelineTransformationRequest generateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("RenameAndMatchStep");
            request.setSubmitter(configuration.getCustomerSpace().getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);

            List<TransformationStepConfig> steps = new ArrayList<>();
            // add the rename step
            TransformationStepConfig rename = rename();
            steps.add(rename);
            // add the match step
            TransformationStepConfig match = match(steps.size() - 1);
            renameAndMatchTablePrefix = "DeleteWith_" + idSystem + "_" + idEntity.name() + "_";
            log.info("RenameAndMatchStep, renameAndMatchTablePrefix: " + renameAndMatchTablePrefix);
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(configuration.getCustomerSpace());
            targetTable.setNamePrefix(renameAndMatchTablePrefix);
            match.setTargetTable(targetTable);
            steps.add(match);

            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String getOutputTableName() {
        return TableUtils.getFullTableName(renameAndMatchTablePrefix, pipelineVersion);
    }

    private TransformationStepConfig rename() {
        TransformationStepConfig step = new TransformationStepConfig();

        step.setTransformer(TRANSFORMER_COPY_TXFMR);
        String sourceTableName = configuration.getTableName();
        SourceTable sourceTable = new SourceTable(sourceTableName, configuration.getCustomerSpace());
        List<String> baseSources = Collections.singletonList(sourceTableName);
        step.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(sourceTableName, sourceTable);
        step.setBaseTables(baseTables);

        CopyConfig config = new CopyConfig();
        Map<String, String> renameAttrs = new HashMap<>();
        renameAttrs.put(sourceTableColumns.get(0), systemIdColumn);
        log.info("RenameAndMatchStep, renameAttrs: " + renameAttrs);
        config.setRenameAttrs(renameAttrs);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    private TransformationStepConfig match(int prevStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(prevStep));
        step.setTransformer(TRANSFORMER_MATCH);
        String matchConfig = getMatchConfig();
        log.info("RenameAndMatchStep, matchConfig: " + matchConfig);
        step.setConfiguration(matchConfig);

        return step;
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(configuration.getCustomerSpace().getTenantId()));
        matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        matchInput.setApplicationId(getApplicationId());
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setAllocateId(false);
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setIncludeLineageFields(true);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        Set<String> columnNames = new HashSet<>();
        columnNames.add(systemIdColumn);
        for (int i = 1; i < sourceTableColumns.size(); i++) {
            columnNames.add(sourceTableColumns.get(i));
        }
        log.info("RenameAndMatchStep, RootOperationUid {}, columnNames {} ", matchInput.getRootOperationUid(),
                columnNames);

        List<String> accountSystemIds = Collections.singletonList(getSystemIdColumn(BusinessEntity.Account, idSystem));
        List<String> contactSystemIds = Collections.singletonList(getSystemIdColumn(BusinessEntity.Contact, idSystem));
        log.info("RenameAndMatchStep, accountSystemIds {}, contactSystemIds {} ", accountSystemIds, contactSystemIds);
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        if (idEntity.equals(BusinessEntity.Account)) {
            MatchInput.EntityKeyMap accountKeyMap = new MatchInput.EntityKeyMap();
            matchInput.setTargetEntity(BusinessEntity.Account.name());
            accountKeyMap.setKeyMap(MatchUtils.getAccountMatchKeysAccount(columnNames, accountSystemIds, false));
            entityKeyMaps.put(BusinessEntity.Account.name(), accountKeyMap);
            matchInput.setEntityKeyMaps(entityKeyMaps);
        } else {
            matchInput.setTargetEntity(BusinessEntity.Contact.name());
            MatchInput.EntityKeyMap accountKeyMap = MatchInput.EntityKeyMap
                    .fromKeyMap(MatchUtils.getAccountMatchKeysForContact(columnNames, accountSystemIds, false, false));
            MatchInput.EntityKeyMap contactKeyMap = MatchInput.EntityKeyMap
                    .fromKeyMap(MatchUtils.getContactMatchKeys(columnNames, contactSystemIds, false));
            matchInput.setEntityKeyMaps(new HashMap<>(ImmutableMap.of( //
                    BusinessEntity.Account.name(), accountKeyMap, //
                    BusinessEntity.Contact.name(), contactKeyMap)));
        }

        config.setMatchInput(matchInput);

        return JsonUtils.serialize(config);
    }

    private String getSystemIdColumn(BusinessEntity idEntity, String idSystem) {
        S3ImportSystem system = cdlProxy.getS3ImportSystem(configuration.getCustomerSpace().toString(), idSystem);
        if (system == null) {
            throw new RuntimeException("RenameAndMatchStep, System " + idSystem + " doesn't exist...");
        }
        String systemIdColumn = idEntity.equals(BusinessEntity.Account) ? system.getAccountSystemId()
                : system.getContactSystemId();

        if (StringUtils.isBlank(systemIdColumn)) {
            throw new RuntimeException("RenameAndMatchStep, system " + idSystem + " is not mapped...");
        }

        return systemIdColumn;
    }
}
