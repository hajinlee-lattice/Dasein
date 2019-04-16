package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanContactExportParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTransactionExportParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.UnmatchedAccountExportParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ComputeOrphanRecordsStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("computeOrphanRecordsStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ComputeOrphanRecordsStep extends RunDataFlow<ComputeOrphanRecordsStepConfiguration> {
    private static Logger log = LoggerFactory.getLogger(ComputeOrphanRecordsStep.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void onConfigurationInitialized() {
        super.onConfigurationInitialized();
        ComputeOrphanRecordsStepConfiguration configuration = getConfiguration();
        configuration.setBeanName(ComputeOrphanRecordsStepConfiguration.DEPENDS_ON_ORPHAN_TYPE);
        log.info(String.format("Bean name=%s. Target table name=%s",
                configuration.getBeanName(), configuration.getTargetTableName()));
        configuration.setDataFlowParams(createDataFlowParameters(configuration));

        // create or update data collection artifact
        String customerSpace = configuration.getCustomerSpace().toString();
        DataCollection.Version version = configuration.getDataCollectionVersion();
        DataCollectionArtifact artifact = dataCollectionProxy.getDataCollectionArtifact(
                customerSpace, configuration.getOrphanRecordsType().getOrphanType(), version);
        if (artifact != null) {
            artifact.setStatus(DataCollectionArtifact.Status.GENERATING);
            artifact = dataCollectionProxy.updateDataCollectionArtifact(customerSpace, artifact);
            log.info(String.format("Updated dataCollectionArtifact of pid=%s, name=%s, url=%s, status=%s)",
                    artifact.getPid(), artifact.getName(), artifact.getUrl(), artifact.getStatus()));
        } else {
            artifact = new DataCollectionArtifact();
            artifact.setName(configuration.getOrphanRecordsType().getOrphanType());
            artifact.setUrl(null);
            artifact.setStatus(DataCollectionArtifact.Status.GENERATING);
            artifact = dataCollectionProxy.createDataCollectionArtifact(customerSpace, version, artifact);
            log.info(String.format("Created dataCollectionArtifact of pid=%s name=%s, url=%s, status=%s",
                    artifact.getPid(), artifact.getName(), artifact.getUrl(), artifact.getStatus()));
        }
    }

    @Override
    public void onExecutionCompleted() {
        Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        log.info(String.format("Target path=%s. Target table name=%s",
                configuration.getTargetPath(), configuration.getTargetTableName()));

        // compute the number of orphan records
        String globPath = targetTable.getExtracts().get(0).getPath() + "/*.avro";
        Long orphanCount = AvroUtils.count(yarnConfiguration, globPath);
        putLongValueInContext(ORPHAN_COUNT, orphanCount);
        log.info(String.format("Found %d orphan records.", orphanCount));
    }

    private DataFlowParameters createDataFlowParameters(ComputeOrphanRecordsStepConfiguration config) {
        DataFlowParameters parameters = null;

        switch (config.getOrphanRecordsType()) {
        case TRANSACTION:
            parameters = new OrphanTransactionExportParameters();
            ((OrphanTransactionExportParameters) parameters).setAccountTable(config.getAccountTableName());
            ((OrphanTransactionExportParameters) parameters).setProductTable(config.getProductTableName());
            ((OrphanTransactionExportParameters) parameters).setTransactionTable(config.getTransactionTableName());
            ((OrphanTransactionExportParameters) parameters).setValidatedColumns(config.getValidatedColumns());
            break;
        case CONTACT:
            parameters = new OrphanContactExportParameters();
            ((OrphanContactExportParameters) parameters).setAccountTable(config.getAccountTableName());
            ((OrphanContactExportParameters) parameters).setContactTable(config.getContactTableName());
            ((OrphanContactExportParameters) parameters).setValidatedColumns(config.getValidatedColumns());
            break;
        case UNMATCHED_ACCOUNT:
            parameters = new UnmatchedAccountExportParameters();
            ((UnmatchedAccountExportParameters) parameters).setAccountTable(config.getAccountTableName());
            ((UnmatchedAccountExportParameters) parameters).setValidatedColumns(config.getValidatedColumns());
            break;
        }

        return parameters;
    }
}
