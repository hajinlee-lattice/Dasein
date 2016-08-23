package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenance;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import edu.emory.mathcs.backport.java.util.Collections;

public class ReconstituteCSVFilesTestNG extends PlsFunctionalTestNGBase {

    private static class Entry {
        public Entry(Table table, ModelSummary summary) {
            this.summary = summary;
            this.table = table;
        }

        public ModelSummary summary;
        public Table table;
    }

    private static final Logger log = Logger.getLogger(ReconstituteCSVFilesTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private ModelProxy modelProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SourceFileService sourceFileService;

    @Test(groups = "manual")
    public void reconstitute() throws IOException {
        SSLUtils.turnOffSslChecking();

        List<Entry> entries = findCandidateTables();
        for (Entry entry : entries) {
            Table table = entry.table;
            ModelSummary summary = entry.summary;
            try {
                log.info(String.format("File doesn't exist for table %s tenant %s .  Reconstituting...",
                        table.getName(), table.getTenant().getId()));
                reconstitute(summary, table);

            } catch (Exception e) {
                log.error(String.format("Failed to reconstitute table %s for tenant %s", table.getName(), table
                        .getTenant().getId()), e);
            }
        }
    }

    private void reconstitute(ModelSummary summary, Table table) throws InterruptedException, IOException {
        String exportPath = PathBuilder.buildDataFilePath("QA", CustomerSpace.parse(table.getTenant().getId()))
                .toString();
        // TODO Set export filename appropriately?
        ExportConfiguration config = new ExportConfiguration();
        config.setTable(table);
        config.setCustomerSpace(CustomerSpace.parse(table.getTenant().getId()));
        config.setExportDestination(ExportDestination.FILE);
        config.setExportFormat(ExportFormat.CSV);
        config.setExportTargetPath(exportPath);

        AppSubmission submission = eaiProxy.createExportDataJob(config);
        while (true) {
            JobStatus status = modelProxy.getJobStatus(submission.getApplicationIds().get(0));
            log.info(String.format("Exporting table %s for tenant %s (Tracking Url: %s)...", table.getName(), table
                    .getTenant().getId(), status.getTrackingUrl()));
            if (status.getStatus() != FinalApplicationStatus.UNDEFINED) {
                if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
                    throw new RuntimeException(String.format("Export (appid %s) (table %s) (tenant %s) failed",
                            submission.getApplicationIds().get(0), table.getName(), table.getTenant().getId()));
                }
                log.info(String.format("Table %s for tenant %s saved to %s", table.getName(),
                        table.getTenant().getId(), exportPath));
                break;
            }
            Thread.sleep(10000);
        }

        SourceFile sourceFile = new SourceFile();
        sourceFile.setDisplayName("source.csv");
        sourceFile.setState(SourceFileState.Imported);
        sourceFile.setName(table.getName());
        sourceFile.setPath(exportPath + "/" + table.getName() + ".csv");
        sourceFile.setApplicationId(submission.getApplicationIds().get(0));
        try {
            sourceFile.setSchemaInterpretation(SchemaInterpretation.valueOf(summary.getSourceSchemaInterpretation()));
        } catch (Exception e) {
            // pass
        }
        sourceFileService.create(sourceFile);
//
//        ModelSummaryProvenance provenance = summary.getModelSummaryConfiguration();
//        provenance.setProvenanceProperty(ProvenancePropertyName.SourceFileName, sourceFile.getName());
//        summary.setModelSummaryConfiguration(provenance);
//        modelSummaryEntityMgr.update(summary);

        if (!HdfsUtils.fileExists(yarnConfiguration, exportPath)) {
            throw new RuntimeException(String.format(
                    "File doesn't seem to have been exported properly.  Output doesn't exist at %s", exportPath));
        }
    }

    @SuppressWarnings("unchecked")
    private List<Entry> findCandidateTables() throws IOException {
        // List<Tenant> tenants = tenantEntityMgr.findAll();
        // TODO Temporary
        Tenant t = tenantEntityMgr.findByTenantId("mytestt.mytestt.Production");
        List<Tenant> tenants = (List<Tenant>)Collections.singletonList(t);
        List<Entry> entries = new ArrayList<>();
        for (Tenant tenant : tenants) {
            log.info(String.format("Tenant %s", tenant.getId()));
            MultiTenantContext.setTenant(tenant);
            List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
            for (ModelSummary summary : summaries) {
                if (!StringUtils.isEmpty(summary.getTrainingTableName())
                        && summary.getTrainingTableName().contains("Source")) {
                    Table table = metadataProxy.getTable(summary.getTenant().getId(), summary.getTrainingTableName());
                    table.setTenant(tenant);

                    SourceFile sourceFile = sourceFileService.findByName(table.getName().replace("SourceFile_", ""));
                    if (sourceFile == null) {
                        entries.add(new Entry(table, summary));
                    }
                }
            }

        }
        return entries;
    }
}
