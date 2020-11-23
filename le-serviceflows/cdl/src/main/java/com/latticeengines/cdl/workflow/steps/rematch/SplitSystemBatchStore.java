package com.latticeengines.cdl.workflow.steps.rematch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Functions;
import com.latticeengines.cdl.workflow.steps.merge.SystemBatchTemplateName;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SplitSystemBatchStoreConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.SplitSystemBatchStoreJob;

@Component(SplitSystemBatchStore.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitSystemBatchStore
        extends RunSparkJob<ConvertBatchStoreStepConfiguration, SplitSystemBatchStoreConfig> {
    private static final Logger log = LoggerFactory.getLogger(SplitSystemBatchStore.class);

    static final String BEAN_NAME = "splitSystemBatchStore";

    public static final String LATTICE_ACCOUNT_ID = InterfaceName.LatticeAccountId.name();

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    LinkedHashSet<String> templates = new LinkedHashSet<>();

    List<String> discardFields;

    private boolean shortCutMode = false;

    @Override
    protected Class<SplitSystemBatchStoreJob> getJobClz() {
        return SplitSystemBatchStoreJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(ConvertBatchStoreStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    protected SplitSystemBatchStoreConfig configureJob(ConvertBatchStoreStepConfiguration stepConfiguration) {
        customerSpace = stepConfiguration.getCustomerSpace();
        Map<String, List<String>> rematchTableMap = getTypedObjectFromContext(REMATCH_TABLE_NAMES,
                new TypeReference<Map<String, List<String>>>() {
                });
        if (rematchTableMap == null) {
            shortCutMode = false;
        } else {
            log.info("rematchTableMap: {}, entity: {}", rematchTableMap, configuration.getEntity().name());
            List<String> rematchTables = rematchTableMap.get(configuration.getEntity().name());
            shortCutMode = CollectionUtils.isEmpty(rematchTables) ? false : true;
        }
        if (shortCutMode) {
            log.info("Found rematch tables in context, going through short-cut mode.");
            return null;
        } else {
            TableRoleInCollection role = (configuration.getEntity() == BusinessEntity.Account)
                    ? TableRoleInCollection.SystemAccount
                    : TableRoleInCollection.SystemContact;

            Table table = dataCollectionProxy.getTable(customerSpace.getTenantId(), role);
            if (table == null) {
                throw new IllegalStateException(
                        "Cannot find " + role.toString() + " for this tenant, initial PA might be missing...");
            } else {
                discardFields = new LinkedList<>();
                // read the table attributes and exact all templates(prefix)
                table.getAttributes().forEach(attr -> {
                    String attrName = attr.getName();
                    // Column LatticeAccountId is generated during matching,
                    // should discard from split imports
                    if (StringUtils.contains(attrName, LATTICE_ACCOUNT_ID)) {
                        discardFields.add(attrName);
                    }
                    int i = attrName.indexOf("__");
                    if (i != -1) {
                        String template = attrName.substring(0, i);
                        templates.add(template);
                    }
                });
            }
            log.info("templates are {}, discard fields are {}", templates, discardFields);
            SplitSystemBatchStoreConfig config = new SplitSystemBatchStoreConfig();
            config.setInput(Collections.singletonList(table.toHdfsDataUnit(role.toString())));
            config.setTemplates(templates.stream().collect(Collectors.toList()));
            config.setDiscardFields(discardFields);
            return config;
        }
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        List<HdfsDataUnit> targets = result.getTargets();
        log.info("Generated {} split tables", targets.size());
        Map<String, List<String>> rematchTables = getTypedObjectFromContext(REMATCH_TABLE_NAMES,
                new TypeReference<Map<String, List<String>>>() {
                });
        if (rematchTables == null) {
            rematchTables = new HashMap<>();
        }

        TableRoleInCollection role = (configuration.getEntity() == BusinessEntity.Account)
                ? TableRoleInCollection.SystemAccount
                : TableRoleInCollection.SystemContact;

        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        Table batchStoreTable = dataCollectionProxy.getTable(customerSpace.toString(),
                configuration.getEntity().getBatchStore(), active);
        Map<String, DataFeedTask> taskMap = MapUtils
                .emptyIfNull(dataFeedProxy.getTemplateToDataFeedTaskMap(customerSpace.toString()));
        int cnt = 1;
        Map<String, String> tableTemplateMap = getMapObjectFromContext(CONSOLIDATE_INPUT_TEMPLATES, String.class,
                String.class);
        if (tableTemplateMap == null) {
            tableTemplateMap = new HashMap<>();
        }
        List<String> tempList = new ArrayList<>(templates);
        log.info("tableTemplateMap size " + tableTemplateMap.size() + ", tempList size " + tempList.size());
        for (HdfsDataUnit target : targets) {
            String tableName = NamingUtils.timestamp(role.toString() + "_split" + cnt);
            log.info("Generated table name is " + tableName);
            Table splitTable = toTable(tableName, target);

            String tmpl = tempList.get(cnt - 1);
            // Update the table template map
            tableTemplateMap.put(tableName, tmpl);

            // make sure table has the same metadata as corresponding template
            if (SystemBatchTemplateName.Other.name().equals(tmpl)
                    || SystemBatchTemplateName.Embedded.name().equals(tmpl)) {
                copyAttrs(splitTable, batchStoreTable);
            } else {
                enrichTable(splitTable, tmpl, taskMap.get(tmpl));
            }

            // Register into metadata table
            metadataProxy.createTable(customerSpace.getTenantId(), tableName, splitTable);

            rematchTables.putIfAbsent(configuration.getEntity().name(), new LinkedList<>());
            rematchTables.get(configuration.getEntity().name()).add(tableName);

            exportToS3(splitTable);
            addToListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);
            cnt++;
        }

        // Put new split tables into context for downstream
        putObjectInContext(REMATCH_TABLE_NAMES, rematchTables);
        // Put the table template map back into context
        putObjectInContext(CONSOLIDATE_INPUT_TEMPLATES, tableTemplateMap);
    }

    private void enrichTable(@NotNull Table table, @NotNull String templateName, DataFeedTask task) {
        if (task == null || task.getImportTemplate() == null) {
            log.warn("Cannot find import template table for {}", templateName);
            return;
        }

        log.info("Enriching split table {} with import template {}, table {}", table.getName(), templateName,
                task.getImportTemplate().getName());
        copyAttrs(table, task.getImportTemplate());
    }

    private void copyAttrs(@NotNull Table target, @NotNull Table source) {
        log.info("Copying {} attribute metadata from table {} to table {} ({} attributes)",
                source.getAttributes().size(), source.getName(), target.getName(), target.getAttributes().size());
        Map<String, Attribute> attrs = source.getAttributes().stream()
                .collect(Collectors.toMap(Attribute::getName, Functions.identity()));
        // use metadata from import template first
        List<Attribute> mergedAttrs = target.getAttributes().stream()
                .map(attr -> attrs.getOrDefault(attr.getName(), attr)).collect(Collectors.toList());
        target.setAttributes(mergedAttrs);
    }
}
