package com.latticeengines.cdl.workflow.steps.rematch;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SplitSystemBatchStoreConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.SplitSystemBatchStoreJob;

@Component(SplitSystemBatchStore.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitSystemBatchStore
        extends RunSparkJob<ConvertBatchStoreStepConfiguration, SplitSystemBatchStoreConfig> {
    private static final Logger log = LoggerFactory.getLogger(SplitSystemBatchStore.class);

    static final String BEAN_NAME = "splitSystemBatchStore";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    LinkedHashSet<String> templates = new LinkedHashSet<>();

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
        TableRoleInCollection role = (configuration.getEntity() == BusinessEntity.Account)
                ? TableRoleInCollection.SystemAccount
                : TableRoleInCollection.SystemContact;

        Table table = dataCollectionProxy.getTable(customerSpace.getTenantId(), role);
        if (table == null) {
            throw new IllegalStateException(
                    "Cannot find " + role.toString() + " for this tenant, initial PA might be missing...");
        } else {
            // read the table attributes and exact all templates(prefix)
            table.getAttributes().forEach(attr -> {
                String attrName = attr.getName();
                int i = attrName.indexOf("__");
                if (i != -1) {
                    String template = attrName.substring(0, i);
                    templates.add(template);
                }
            });
        }
        log.info("templates are " + templates);
        SplitSystemBatchStoreConfig config = new SplitSystemBatchStoreConfig();
        config.setInput(Collections.singletonList(table.toHdfsDataUnit(role.toString())));
        config.setTemplates(templates.stream().collect(Collectors.toList()));
        config.setDiscardFields(configuration.getDiscardFields());
        return config;
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

        int cnt = 1;
        Map<String, String> tableTemplateMap = getMapObjectFromContext(CONSOLIDATE_INPUT_TEMPLATES, String.class,
                String.class);
        if (tableTemplateMap == null) {
            tableTemplateMap = new HashMap<>();
        }
        List<String> tempList = templates.stream().collect(Collectors.toList());
        log.info("tableTemplateMap size " + tableTemplateMap.size() + ", tempList size " + tempList.size());
        for (HdfsDataUnit target : targets) {
            String tableName = NamingUtils.timestamp(role.toString() + "_split" + cnt);
            log.info("Generatd table name is " + tableName);
            Table splitTable = toTable(tableName, target);

            // Update the table template map
            tableTemplateMap.put(tableName, tempList.get(cnt - 1));

            // Register into metadata table
            metadataProxy.createTable(customerSpace.getTenantId(), tableName, splitTable);

            rematchTables.putIfAbsent(configuration.getEntity().name(), new LinkedList<String>());
            rematchTables.get(configuration.getEntity().name()).add(tableName);
            cnt++;
        }

        // Put new split tables into context for downstream
        putObjectInContext(REMATCH_TABLE_NAMES, rematchTables);
        // Put the table template map back into context
        putObjectInContext(CONSOLIDATE_INPUT_TEMPLATES, tableTemplateMap);
    }
}
