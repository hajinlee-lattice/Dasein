package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedWebVisit;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.spark.exposed.job.common.CopyJob;

@Component(FinishReportGenerationStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishReportGenerationStep extends BaseSparkStep<ProcessStepConfiguration> {

    private static Logger log = LoggerFactory.getLogger(FinishReportGenerationStep.class);

    static final String BEAN_NAME = "finishReportGenerationStep";

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;

    @Override
    public void execute() {
        if (BooleanUtils.isNotTrue(getObjectFromContext(IS_SSVI_TENANT, Boolean.class))
                || BooleanUtils.isTrue(getObjectFromContext(IS_CDL_TENANT, Boolean.class))) {
            log.info("Not SSVI only tenant, skipping");
            return;
        }
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        convertWebVisitTableToParquet();

        CustomerSpace customerSpace = configuration.getCustomerSpace();
        cloneTableService.setCustomerSpace(customerSpace);
        cloneTableService.setActiveVersion(active);
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null) {
            cloneTableService.setRedshiftPartition(dcStatus.getRedshiftPartition());
        }
        cloneTableService.linkToInactiveTableWithSignature(TableRoleInCollection.ConsolidatedCatalog);
        cloneTableService.linkInactiveTable(TableRoleInCollection.ConsolidatedWebVisit);
    }

    private void convertWebVisitTableToParquet() {
        String customer = configuration.getCustomerSpace().toString();
        if (!hasTableInCtxKey(customer, SSVI_WEBVISIT_RAW_TABLE)) {
            log.info("No web visit table in context {}, not converting", SSVI_WEBVISIT_RAW_TABLE);
            return;
        }

        Table webVisitTable = getTableSummaryFromKey(customer, SSVI_WEBVISIT_RAW_TABLE);

        // convert to parquet
        log.info("Converting web visit table {} from avro to parquet", webVisitTable.getName());
        CopyConfig copyConfig = new CopyConfig();
        copyConfig.setInput(Collections.singletonList(webVisitTable.toHdfsDataUnit("WebVisit")));
        copyConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        SparkJobResult result = runSparkJob(CopyJob.class, copyConfig);
        HdfsDataUnit parquetWebVisit = result.getTargets().get(0);

        // create table and link to inactive version
        String parquetWebVisitTableName = NamingUtils.timestamp(ConsolidatedWebVisit.name());
        log.info("Web visit parquet table {} converted successfully. path = {}", parquetWebVisitTableName,
                parquetWebVisit.getPath());
        Table table = toTable(parquetWebVisitTableName, parquetWebVisit);
        metadataProxy.createTable(customer, parquetWebVisitTableName, table);
        boolean skipped = exportToS3(table, true, DataUnit.DataFormat.PARQUET);
        if (skipped) {
            log.info("Skip exporting web visit table to s3");
        } else {
            RetryTemplate template = RetryUtils.getExponentialBackoffRetryTemplate(10, 2000, 1.5, null);
            template.execute(
                    ctx -> dataUnitProxy.registerAthenaDataUnit(customerSpace.toString(), parquetWebVisitTableName));
        }
        dataCollectionProxy.upsertTable(customerSpace.toString(), parquetWebVisitTableName, ConsolidatedWebVisit,
                inactive);
    }
}
