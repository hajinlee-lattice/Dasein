package com.latticeengines.cdl.workflow.steps.merge;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("initializeTransaction")
public class InitializeTransaction extends BaseWorkflowStep<ProcessTransactionStepConfiguration> {

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        initOrClonePeriodStore(TableRoleInCollection.ConsolidatedRawTransaction, SchemaInterpretation.TransactionRaw);
        initOrClonePeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction,
                SchemaInterpretation.TransactionDailyAggregation);
        initOrClonePeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction,
                SchemaInterpretation.TransactionPeriodAggregation);
    }

    private void initOrClonePeriodStore(TableRoleInCollection role, SchemaInterpretation schema) {
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        if (StringUtils.isNotBlank(activeTableName)) {
            log.info("Cloning " + role + " from " + active + " to " + inactive);
            clonePeriodStore(role);
        } else {
            // no active store to clone
            log.info("Building a brand new " + role);
            buildPeriodStore(role, schema);
        }
    }

    private void clonePeriodStore(TableRoleInCollection role) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private Table buildPeriodStore(TableRoleInCollection role, SchemaInterpretation schema) {
        Table table = SchemaRepository.instance().getSchema(schema);
        String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString();

        try {
            log.info("Initialize period store " + hdfsPath + "/" + schema);
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + schema);
        } catch (Exception e) {
            log.error("Failed to initialize period store " + hdfsPath + "/" + schema);
            throw new RuntimeException("Failed to create period store " + role);
        }

        Extract extract = new Extract();
        extract.setName("extract_target");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/" + schema + "/");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), role, inactive);
        log.info("Upsert table " + table.getName() + " to role " + role + "version" + inactive);

        return table;
    }

}
