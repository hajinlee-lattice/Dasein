package com.latticeengines.serviceflows.workflow.importvdbtable;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.*;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.domain.exposed.metadata.VdbImportStatus;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@Component("importVdbTable")
public class ImportVdbTable extends BaseWorkflowStep<ImportVdbTableStepConfiguration> {

    private static final Log log = LogFactory.getLog(ImportVdbTable.class);

    public static final String EXTRACT_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Start import Vdb table.");

        try {
            importVdbTable();
        } catch (Exception e) {
            VdbLoadTableStatus status = new VdbLoadTableStatus();
            status.setMessage(String.format("Load vdb table failed: %s", e.getMessage()));
            status.setJobStatus("Failed");
            status.setVisiDBQueryHandle(configuration.getVdbQueryHandle());
            reportStatus(configuration.getReportStatusEndpoint(), status);
            updateVdbImportExtract(VdbImportStatus.FAILED, "");
        }
    }

    private void importVdbTable() throws Exception {
        EaiJobConfiguration importConfig = setupConfiguration();
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        updateVdbImportExtract(VdbImportStatus.RUNNING, applicationId);
        waitForAppId(applicationId);
    }

    private ImportConfiguration setupConfiguration() throws Exception {
        ImportConfiguration importConfig = new ImportConfiguration();

        importConfig.setCustomerSpace(configuration.getCustomerSpace());
        importConfig.setProperty(ImportVdbProperty.DATA_CATEGORY, configuration.getDataCategory());
        importConfig.setProperty(ImportVdbProperty.MERGE_RULE, configuration.getMergeRule());
        importConfig.setProperty(ImportVdbProperty.QUERY_DATA_ENDPOINT, configuration.getGetQueryDataEndpoint());
        importConfig.setProperty(ImportVdbProperty.REPORT_STATUS_ENDPOINT, configuration.getReportStatusEndpoint());
        importConfig.setProperty(ImportVdbProperty.TOTAL_ROWS, String.valueOf(configuration.getTotalRows()));
        importConfig.setProperty(ImportVdbProperty.BATCH_SIZE, String.valueOf(configuration.getBatchSize()));
        importConfig.setProperty(ImportVdbProperty.VDB_QUERY_HANDLE, configuration.getVdbQueryHandle());
        importConfig.setProperty(ImportVdbProperty.EXTRACT_IDENTIFIER, configuration.getExtractIdentifier());
        importConfig.setProperty(ImportVdbProperty.METADATA_LIST, JsonUtils.serialize(configuration.getMetadataList()));

        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.VISIDB);

        String targetPath = getExtractTargetPath();

        String extractName = String.format("Extract_%s.avro", configuration.getTableName());

        importConfig.setProperty(ImportVdbProperty.TARGETPATH, targetPath);
        importConfig.setProperty(ImportVdbProperty.EXTRACT_NAME, extractName);

        Table table = createTable(extractName, targetPath, configuration.getCreateTableRule());
        sourceImportConfig.setProperty(ImportProperty.METADATA, //
                JsonUtils.serialize(table.getModelingMetadata()));
        sourceImportConfig.setTables(Arrays.asList(table));
        importConfig.addSourceConfiguration(sourceImportConfig);
        return importConfig;
    }

    private Table createTable(String extractName, String extractPath, VdbCreateTableRule createRule) throws Exception {
        Table table;
        table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), "Vdb_" + configuration.getTableName());
        if (table == null) {
            if (createRule == VdbCreateTableRule.UPDATE) {
                throw new Exception(String.format("Table %s not exist, cannot import table with update rule",
                        configuration.getTableName()));
            }
            table = new Table();
        } else {
            if (createRule == VdbCreateTableRule.CREATE_NEW) {
                throw new Exception(String.format("Table %s already exist, cannot create a new one",
                        configuration.getTableName()));
            }
        }
        if (verifyAndUpdateTableAttr(table, configuration.getMetadataList())) {
            table.setPrimaryKey(null);
            table.setName("Vdb_" + configuration.getTableName());
            table.setDisplayName(configuration.getTableName());
        } else {
            throw new Exception("Table metadata is conflict with table already existed.");
        }
        return table;
    }

    private boolean verifyAndUpdateTableAttr(Table table, List<VdbSpecMetadata> metadataList) {
        boolean result = true;
        HashMap<String, Attribute> attrMap = new HashMap<>();
        for (Attribute attr : table.getAttributes()) {
            attrMap.put(attr.getName(), attr);
        }
        for (VdbSpecMetadata metadata : metadataList) {
            if (!attrMap.containsKey(metadata.getColumnName())) {
                Attribute attr = new Attribute();
                attr.setName(metadata.getColumnName());
                attr.setDisplayName(metadata.getDisplayName());
                attr.setLogicalDataType(metadata.getDataType());
                table.addAttribute(attr);
            } else {
                if (!attrMap.get(metadata.getColumnName()).getSourceLogicalDataType().toLowerCase()
                        .equals(metadata.getDataType().toLowerCase())) {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    private void reportStatus(String url, VdbLoadTableStatus status) {
        try {
            restTemplate.postForEntity(url, status, Void.class);
        } catch (Exception e) {
            log.warn("Error reporting status");
        }
    }

    private void updateVdbImportExtract(VdbImportStatus status, String applicationId) {
        String customSpace = configuration.getCustomerSpace().toString();
        String extractIdentifier = configuration.getExtractIdentifier();
        VdbImportExtract vdbImportExtract = metadataProxy.getVdbImportExtract(customSpace, extractIdentifier);
        if (vdbImportExtract != null) {
            vdbImportExtract.setStatus(status);
            if (!StringUtils.isEmpty(applicationId)) {
                vdbImportExtract.setLoadApplicationId(applicationId);
            }
            metadataProxy.updateVdbImportExtract(customSpace, vdbImportExtract);
        }
    }

    private String getExtractTargetPath() {
        String customSpace = configuration.getCustomerSpace().toString();
        String extractIdentifier = configuration.getExtractIdentifier();
        VdbImportExtract vdbImportExtract = metadataProxy.getVdbImportExtract(customSpace, extractIdentifier);
        String targetPath = "";
        if (vdbImportExtract != null) {
            targetPath = vdbImportExtract.getTargetPath();

        }
        if (StringUtils.isEmpty(targetPath)) {
            targetPath = String.format("%s/%s/%s/Extracts/%s",
                    PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace()).toString(),
                    SourceType.VISIDB.getName(), configuration.getTableName(),
                    new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        }
        vdbImportExtract.setTargetPath(targetPath);
        metadataProxy.updateVdbImportExtract(customSpace, vdbImportExtract);
        return targetPath;
    }
}
