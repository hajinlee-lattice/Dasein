package com.latticeengines.cdl.workflow.steps.export;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("exportDataToRedshift")
public class ExportDataToRedshift extends BaseWorkflowStep<ExportDataToRedshiftConfiguration> {

    private static final Log log = LogFactory.getLog(ExportDataToRedshift.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    private Table sourceTable;
    private boolean needToSplit;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");

        boolean shouldSkip = configuration.isSkipStep()
                || Boolean.TRUE.equals(getObjectFromContext(DATA_INITIAL_LOADED, Boolean.class));
        log.info("shouldSkip=" + shouldSkip);
        if (!shouldSkip) {
            sourceTable = getObjectFromContext(TABLE_GOING_TO_REDSHIFT, Table.class);
            needToSplit = getObjectFromContext(SPLIT_LOCAL_FILE_FOR_REDSHIFT, Boolean.class);
            if (sourceTable == null) {
                sourceTable = configuration.getSourceTable();
            }
            renameTable();
            exportData();
        }
    }

    private void exportData() {
        EaiJobConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0).toString());
        waitForAppId(submission.getApplicationIds().get(0).toString());
    }

    private void renameTable() {
        String goodName = AvroUtils.getAvroFriendlyString(sourceTable.getName());
        if (!goodName.equalsIgnoreCase(sourceTable.getName())) {
            log.info("Renaming table " + sourceTable.getName() + " to " + goodName);
            sourceTable.setName(goodName);
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), goodName, sourceTable);
        }
    }

    private ExportConfiguration setupExportConfig() {
        HdfsToRedshiftConfiguration exportConfig = configuration.getHdfsToRedshiftConfiguration();
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setNoSplit(!needToSplit);
        exportConfig.setExportDestination(ExportDestination.RedShift);
        RedshiftTableConfiguration redshiftTableConfig = exportConfig.getRedshiftTableConfiguration();
        redshiftTableConfig.setTableName(sourceTable.getName());
        redshiftTableConfig.setJsonPathPrefix(
                String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, sourceTable.getName()));
        return exportConfig;
    }

}
