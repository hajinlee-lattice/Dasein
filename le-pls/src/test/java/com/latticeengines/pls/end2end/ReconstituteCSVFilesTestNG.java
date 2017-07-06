package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.parallel.Parallel;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ReconstituteCSVFilesTestNG extends PlsFunctionalTestNGBase {

    public static class Entry {
        public Entry(Table table, ModelSummary summary) {
            this.summary = summary;
            this.table = table;
        }

        public Entry() {
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
        Deque<Entry> entries = findCandidateTables();
        Parallel.foreach(entries, new Parallel.Operation<Entry>() {
            @Override
            public void perform(Entry entry) {
                Table table = entry.table;
                ModelSummary summary = entry.summary;
                try {
                    log.info(String.format("File doesn't exist for table %s tenant %s summary %s.  Reconstituting...",
                            table.getName(), summary.getTenant().getId(), summary.getDisplayName()));
                    reconstitute(summary, table);

                } catch (Exception e) {
                    log.error(String.format("Failed to reconstitute table %s for tenant %s", table.getName(), summary
                            .getTenant().getId()), e);
                }
            }
        });
    }

    private void reconstitute(ModelSummary summary, Table table) throws InterruptedException, IOException {
        MultiTenantContext.setTenant(summary.getTenant());
        String exportPath = PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(summary.getTenant().getId()))
                .append(table.getName()).toString();
        List<String> paths = HdfsUtils.getFilesByGlob(yarnConfiguration, exportPath + "*");
        if (paths.size() == 1) {
            log.warn(String.format("Cleaning up old orphaned file at path %s", paths.get(0)));
            HdfsUtils.rmdir(yarnConfiguration, paths.get(0));
        }

        ExportConfiguration config = new ExportConfiguration();
        config.setTable(table);
        config.setCustomerSpace(CustomerSpace.parse(summary.getTenant().getId()));
        config.setExportDestination(ExportDestination.FILE);
        config.setExportFormat(ExportFormat.CSV);
        config.setExportTargetPath(exportPath);

        AppSubmission submission = eaiProxy.submitEaiJob(config);
        while (true) {
            JobStatus status = modelProxy.getJobStatus(submission.getApplicationIds().get(0));
            log.info(String.format("Exporting table %s for tenant %s (Tracking Url: %s)...", table.getName(), summary
                    .getTenant().getId(), status.getTrackingUrl()));
            if (status.getStatus() != FinalApplicationStatus.UNDEFINED) {
                if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
                    throw new RuntimeException(String.format("Export (appid %s) (table %s) (tenant %s) failed",
                            submission.getApplicationIds().get(0), table.getName(), summary.getTenant().getId()));
                }
                log.info(String.format("Table %s for tenant %s saved to %s", table.getName(), summary.getTenant()
                        .getId(), exportPath));
                break;
            }
            Thread.sleep(10000);
        }
        paths = HdfsUtils.getFilesByGlob(yarnConfiguration, exportPath + "*");
        if (paths.size() == 0) {
            throw new RuntimeException(String.format(
                    "File doesn't seem to have been exported properly.  Output doesn't exist at %s", exportPath));
        }
        if (paths.size() > 1) {
            throw new RuntimeException(String.format("Multiple files matched with glob %s", exportPath + "*"));
        }

        SourceFile sourceFile = new SourceFile();
        sourceFile.setDisplayName("source.csv");
        sourceFile.setState(SourceFileState.Imported);
        sourceFile.setName(table.getName());
        sourceFile.setPath(paths.get(0));
        sourceFile.setApplicationId(submission.getApplicationIds().get(0));
        sourceFile.setTableName(table.getName());
        sourceFile.setDescription("Autogenerated by upgrade script");
        try {
            sourceFile.setSchemaInterpretation(SchemaInterpretation.valueOf(summary.getSourceSchemaInterpretation()));
        } catch (Exception e) {
            // pass
        }
        sourceFileService.create(sourceFile);
        log.info(String.format("Successfully generated source file %s for table %s", sourceFile.getName(),
                sourceFile.getTableName()));
    }

    private Deque<Entry> findCandidateTables() throws IOException {
        // TODO Convert to query
        List<String> ids = new ArrayList<>();

        ids.add("ms__aa9639c4-e70c-4e51-a587-0a61068500ac-Migratio");
        ids.add("ms__51a406a5-5bd8-4c97-833c-ac027ec72872-Migratio");
        ids.add("ms__176ef9d9-9230-4890-b6af-955aca97ad39-LP2-Clon");
        ids.add("ms__26dd468a-414f-492c-9cfe-b3fb7e29e5a7-Migratio");
        ids.add("ms__07f24d7f-8128-481b-aec7-61553ec76488-LP2-Clon");
        ids.add("ms__cd17beab-c6ea-412d-841c-3ae5572affd1-LP2-Clon");
        ids.add("ms__950ca287-91bd-449b-92bb-3c02a94efcdc-LP2-Clon");
        ids.add("ms__ac8c6753-5e52-4772-baf6-8f41b5cd6f22-LP2-Clon");
        ids.add("ms__cba5d0d3-ebd6-4f93-be66-eda5d3635da8-Lattice-");
        ids.add("ms__2603ce42-dc48-4671-8f72-78c59f572638-SureShot");
        ids.add("ms__e9f4686d-9c37-4813-9361-ad47fcaa96ec-LP2-Clon");
        ids.add("ms__cacb9748-b655-4c2f-86d4-a07378fcf548-LP2-Clon");
        ids.add("ms__e05fcdc5-aaa0-46d9-abe5-b82e0a899d8d-LP2-Clon");
        ids.add("ms__d3cb8ed3-19f5-4dd3-acc4-5244997f222b-LP2-Clon");
        ids.add("ms__b774d309-e6f4-4980-b669-697e5ea8cece-LP2-Clon");
        ids.add("ms__f787b9ed-6a29-4821-8927-40b41371e936-LP2-Clon");
        ids.add("ms__5e53d3f4-0e9e-4e51-894a-4fce2c015c2a-LP2-Clon");
        ids.add("ms__d99cf001-0f5f-4321-977c-2175ebbe4c43-LP2-Clon");
        ids.add("ms__2499cfdd-e31f-4033-aafc-136ff382b34f-Jim-Mule");
        ids.add("ms__1f3b33fc-b1e6-4e14-9311-951b4d5b255b-Jeff-Mod");
        ids.add("ms__2f8978e2-00b2-4db1-ac52-320bdf9cd851-SureShot");
        ids.add("ms__893b6c9b-8458-46ab-8459-05652fee19ab-LP2-Clon");
        ids.add("ms__2f47034f-167f-428e-a1ec-b68d7c23d7db-LP2-Clon");
        ids.add("ms__293c8ba2-1725-46f6-be3f-88c02253e26f-SureShot");
        ids.add("ms__edac4584-bed2-4ec5-b3da-552d76588465-Leads---");
        ids.add("ms__5fe2b802-175c-4d3e-a8fc-d39268850761-Leads---");
        ids.add("ms__622863e2-ab7c-4f30-b498-fc4252e5bb1d-US-Ent-M");
        ids.add("ms__ebe9d39d-e23b-46ee-8e8a-f2fc58a8d0f1-US-Ent-M");
        ids.add("ms__cdb1872a-ec2b-4fd5-b475-06ac9c087afb-Demo-Mod");
        ids.add("ms__59bce8d6-a048-4cb9-9551-e3355c27fa6f-Clone-De");
        ids.add("ms__6eb86a96-636b-4422-8901-169ab09c80f2-Leads---");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D");
        ids.add("ms__6eb86a96-636b-4422-8901-169ab09c80f2-Leads----001");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D-001");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D-002");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D-003");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D-004");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D-005");
        ids.add("ms__a15647d8-a7c9-47ab-bb20-3b4a6f8f5ec2-Refine-D-006");
        ids.add("ms__d9d9686f-080e-489f-a297-f7457d20ca6d-Latest-L");
        ids.add("ms__9f0287b5-df92-4ab6-af20-4591d1d47901-June-7-L");
        ids.add("ms__20db1f1c-5018-47d3-b632-4593625a4ead-LP2-Clon-003");
        ids.add("ms__ab03a95a-fe59-4dcc-a489-cadb24617a93-NoIMomSu");
        ids.add("ms__608a9854-c170-425c-bd76-01bdba66f917-LP2-Clon");
        ids.add("ms__20db1f1c-5018-47d3-b632-4593625a4ead-LP2-Clon");
        ids.add("ms__20db1f1c-5018-47d3-b632-4593625a4ead-LP2-Clon-001");
        ids.add("ms__20db1f1c-5018-47d3-b632-4593625a4ead-LP2-Clon-002");
        ids.add("ms__18fa4955-975a-41bd-a758-9d16854719ce-Lattice-");
        ids.add("ms__d2e04d4a-8c67-4a01-8dc5-f2d85e7751de-LP2-Clon");
        ids.add("ms__ca3277f7-4c01-4df7-881d-aa621a803279-Includes");
        ids.add("ms__547006a4-fa73-498d-a6d2-f22e25f23b8a-HootSuit");
        ids.add("ms__9fd8853c-8ddf-4dc5-8696-7ad32590f93a-HootSuit");
        ids.add("ms__d4d9c1de-2739-448d-bbd5-4d0983f22c4e-LatticeR");
        ids.add("ms__533fa733-d290-4809-a054-bb4d3b644337-HootSuit");
        ids.add("ms__91a4c56a-093c-4034-8268-f562911a1e4f-Alpha-No");
        ids.add("ms__d2562577-52b9-49bb-9f9c-328380d1e893-Alpha-Wi");
        ids.add("ms__b285af06-b2b4-403d-9a5a-9ac3b591fc81-Alpha-No");
        ids.add("ms__c5b47b30-1b77-4789-8e79-f117c790497a-FengTest");
        ids.add("ms__754ca561-ddff-459a-9503-7b06d275256a-ExcludeH");
        ids.add("ms__c4010160-ff64-4655-aed8-6f5318e0f9fc-USModel");
        ids.add("ms__8fd28fb3-bd92-4c1b-a2e8-f9f176e3caf1-US-Model");
        ids.add("ms__f1d1da4b-5c34-4ddf-9e1b-1d5b63c54078-US-Model");
        ids.add("ms__d4d5c841-1229-4449-ba11-f9b50ba2c8d7-Mulesoft");
        ids.add("ms__1d9ced8a-ad91-43be-8368-dce67116d5b5-Fireeye-");
        ids.add("ms__384e03ec-3448-4b98-a202-089b0e423e17-account");
        ids.add("ms__1f98fb5e-7a4f-467d-9d49-4efea5c5fb61-lead");
        ids.add("ms__2b35008f-759e-414a-ae88-0f6f58e74881-RTS-Test");
        ids.add("ms__f44c2d20-d383-4010-b5a6-37b54e78becd-Model-6");
        ids.add("ms__1745ff2f-d6dc-4452-bb35-380fb9363100-Depploym");
        ids.add("ms__4851235d-66d4-4d4c-9692-431761ae40db-TestMode");
        ids.add("ms__d7b9594e-30c5-496c-ad83-89bd8159a0a9-Model-Co");
        ids.add("ms__f142f612-0b4f-4db0-a48d-111b58d506bc-Model-Co");
        ids.add("ms__b853c249-10a7-4142-84a4-e858b6840c1f-Event--C");
        ids.add("ms__d82cdde9-50e6-4104-915b-712f9b24d426-V4--All-");
        ids.add("ms__d0603800-1f77-4dff-9d8a-33342f26d582-V4--All-");
        ids.add("ms__7672211c-2c58-45bb-9d11-cf49c49b427f-V2--All-");
        ids.add("ms__9ec2c5a7-0ed7-41ed-bab5-030ddbff9f67-V3--All-");
        ids.add("ms__8803233d-f45a-476f-a350-09ace1232e39-Qlik-Ver");
        ids.add("ms__ce9877b5-c6f5-49ac-b0f7-122ff4dfa05c-Qlik-Ver");
        ids.add("ms__2ab8394b-282f-4706-afd9-d49f7c971bad-fds");
        ids.add("ms__3e2f4745-f30c-4138-9e5a-af5dbf812d8c-V1--All-");
        ids.add("ms__4d96fb37-9e9e-4477-bf6b-577a73b037e9-V2--All-");
        ids.add("ms__12dda057-1942-40cf-b7a3-591c4a3acfcd-V3--All-");
        ids.add("ms__c9be27d9-1d2c-49fc-b50f-0d4c256aaa0a-RTS-Acco");
        ids.add("ms__c64d3fc7-9a79-4e98-bf00-1e6b69b9fe62-account_");
        ids.add("ms__fe8f7e72-a9e4-4121-83fe-00b18bfe696a-OneLeadP");
        ids.add("ms__8167b7bb-f821-4ed2-80d8-32a15935f2a9-account_");
        ids.add("ms__3fa4426e-dc66-41a7-a2bb-023f5733a6a8-OneLeadP");
        ids.add("ms__5e6215bc-42ea-401b-907a-29d2c3bf4da4-Alpha-Al");
        ids.add("ms__3b0fc3a6-358d-42a7-8b4e-d42748dbaa74-Alpha-No");
        ids.add("ms__0f26eb92-ffdf-4db1-9342-023f2f75afb1-Alpha-No");
        ids.add("ms__bbc4d9ff-8246-4fd4-a534-a841f2afca13-US-Enter");
        ids.add("ms__bd17ce15-a976-4558-a1bc-1c6887c99980-US-Lead-");
        ids.add("ms__928c8d18-fe90-44d8-b327-aba3abb68d34-account_");
        ids.add("ms__f5ef823a-a2a8-4ff4-be1d-3993786ea8e7-OneLeadP");
        ids.add("ms__d9a96e73-e0e8-429f-aa42-1e4f5fe35584-ysong");
        ids.add("ms__f73ae2bd-21d9-4b1a-a658-7b73886e708a-NA-Initi");
        ids.add("ms__4ffa7483-e033-4fc9-905a-d4df8147815c-NA-Remov");
        ids.add("ms__b1b3210a-2677-4b9a-8877-3704e9a06044-NA-Remov");
        ids.add("ms__36a240cd-850d-462e-a0d2-9be68cc98f37-account_");
        ids.add("ms__6541de2d-975d-4011-8442-009e6bb516a4-OneLeadP");
        ids.add("ms__e72188be-090c-4cd8-b3b9-e89ba0a53dd8-account_");
        ids.add("ms__f761bc33-f6d7-4bad-ac3e-eb54b7f1e06c-OneLeadP");
        ids.add("ms__66ba0e68-8f7b-41f2-b727-fa4d1a678ade-account_");
        ids.add("ms__29ad82c7-ffd9-4619-9b14-905325ed39c5-OneLeadP");
        ids.add("ms__fb3a9f62-c364-4be8-90a1-a827378159c7-V4---Eme");
        ids.add("ms__5697ea35-ea5d-4fab-98e7-4e7f1ea44fb6-ICIMS-Em");
        ids.add("ms__6a689e19-8113-4b75-ade8-e18740031b07-ICIMS-Em");
        ids.add("ms__ee4cb760-cb53-4725-991c-66b5b281f254-ICIMS-Al");
        ids.add("ms__26b82751-2f63-4aba-b4c0-2f46a6462742-iCIMS-Al");
        ids.add("ms__85c14096-a09c-4bf4-a8b2-bf93205d8663-ICIMS-Ma");
        ids.add("ms__974d0508-1b73-4cf6-ab4f-d28ff4a83dc1-ICIMS-Ma");
        ids.add("ms__d09a039a-18ec-40ed-89c9-f8b0e71e6681-US-Comme");
        ids.add("ms__a3f8eea2-cda3-4bb9-b170-85b019d73cc7-ICIMS-En");
        ids.add("ms__89bb86a4-e09a-43eb-ad59-f9d2793bc2e9-NA-InclP");
        ids.add("ms__8d20ea47-2b43-485d-b745-9cd59d6c0e7c-NA-InclP");
        ids.add("ms__4b5d21ac-5f8a-4ce7-a0d0-2716501be45a-ICIMS-Em");
        ids.add("ms__ebb3e156-4131-40ff-bf7a-93bc60c62194-iCIMS-Al");
        ids.add("ms__d3613b32-2f92-4d5e-9555-c70a9d6d7dd4-ICIMS-Em");
        ids.add("ms__c414b2bc-5718-4fd4-a263-c683729323b0-ICIMS-Al");
        ids.add("ms__a6a011e6-0a34-4f66-bba4-de064773e7dc-ICIMS-Al");
        ids.add("ms__9d7a0273-d092-487f-9c82-890bd03f22e7-Target-A");
        ids.add("ms__8dff1992-75fc-4363-b44f-923836d19bca-Target-A");
        ids.add("ms__41345c1f-e522-4b57-86e8-ced39c38f5f8-Sirius-2");
        ids.add("ms__da2b056f-1f18-462f-aa7d-30b1c27bda8c-Lattice-");
        ids.add("ms__488ab157-05e9-47c0-9330-0c1b91a3ade4-Regressi");
        ids.add("ms__67d480d4-966a-4449-9a7a-6bc1f332ea9a-OneLeadP");
        ids.add("ms__9c36fe53-622c-4b22-b512-35d79b4c1770-Model");
        ids.add("ms__17147467-a576-410c-aed2-ed9f437adb3d-Take-2");
        ids.add("ms__15296250-6caf-4bb2-954b-4ac3e5aee4e9-Lattice-");
        ids.add("ms__9cec01a6-404f-4eb5-96da-b801162dea03-Closed-W");
        ids.add("ms__50809137-6594-4757-adea-84268392749c-Closed-W");
        ids.add("ms__e6ede61d-b29f-4df2-b4b6-e004d46931ff-Closed-W");
        ids.add("ms__881efd8f-aeb8-4a55-bda4-897f62b295f2-Smoke");
        ids.add("ms__f81215d8-2a16-4d8d-853c-554c5f17bf1e-end2end");
        ids.add("ms__1d26a53a-1b71-4e4a-822c-6f4b62ae2b66-Lead_146");
        ids.add("ms__56651161-b638-4609-86b5-ab8801d3a71a-PCOM---S");
        ids.add("ms__1874368d-8a38-4bb1-bfc3-8024e7bf2de3-Lattice-");
        ids.add("ms__fdeb20a8-c85c-437a-a79e-ac1829360ce4-Tenable-");
        ids.add("ms__ccabd385-1c12-48f3-98b1-3c69c3b5bca1-Tenable-");
        ids.add("ms__f705f6bc-fae9-40fe-ac13-8489c5675191-Lead_146");
        ids.add("ms__0bff4510-3b32-427a-9109-cb8f34a2b79d-InclPubD");
        ids.add("ms__f76caae2-9b5d-496e-9ec8-c2f92073c4a3-InclPubD");
        ids.add("ms__b6018729-a805-4b02-ac42-ddf1e5587238-Activity");
        ids.add("ms__d4f81e6f-efe3-4b02-a708-9cd6dda38ebd-InclPubD");
        ids.add("ms__e9b4d42b-a7e0-4146-8de8-f000ebe1f793-InclPubD");
        ids.add("ms__ea7656d5-0838-410f-93ba-d104f83580e4-InclPubD");
        ids.add("ms__d5df8c53-a508-4510-aa65-9085e4b7e682-Lead_146");
        ids.add("ms__18ad1461-2a9b-4ddb-bce8-805e40b59de4-Tenable-");
        ids.add("ms__4c394c90-be1c-4e27-b58f-c4ab2fe35e98-Tenable-");
        ids.add("ms__5958a4d0-122e-4c13-87bb-e6109a1474dd-Nipul-Mo");
        ids.add("ms__7631dc10-2d65-4ac6-af74-5910ca12579e-Nipul-Mo");
        ids.add("ms__0a0d7c7d-9518-4e96-93f5-48627694a6c1-Nipul-Le");
        ids.add("ms__f268d018-f1a0-42c7-b5a2-96f23aee514b-Clone");
        ids.add("ms__82d9b8af-974a-402e-94eb-fd7ca3ccc0d0-Fireeye-");
        ids.add("ms__39ad7e9c-5c49-4f82-ac30-233baeac361b-Qualys-N");
        ids.add("ms__9dae379d-1c91-4ad6-96e4-374c8482f49f-Qualys-N");
        ids.add("ms__9fdfbf29-30a8-4298-af0a-80aedc92c816-Lead_146");
        ids.add("ms__9581b095-0372-4a51-a92d-ad4b9a648bd8-Lead_146");
        ids.add("ms__f200f9c7-2f55-47cf-97c2-4b98fe9daeaa-Lead_146");
        ids.add("ms__917de1a0-e100-42fd-8da3-a9565372dee6-Lead_146");
        ids.add("ms__c7644447-3a2b-45ac-ab4a-4bc238b3e379-Lead_146");
        ids.add("ms__8f27789a-78fe-4493-b492-68aa68ddfd78-Activity");
        ids.add("ms__2f65655b-6b2e-4626-ad61-c7f345b0b2c3-Lead_146");
        ids.add("ms__1adb4b38-555f-4870-9645-d2f45e09063e-Activity");
        ids.add("ms__f5597199-373a-4085-a20f-c5878cf244d5-Activity");
        ids.add("ms__fee46fb1-5ecf-4633-8311-12c164bb81b1-Lead_146");
        ids.add("ms__9960eb1b-0eb1-4d4e-9213-3951c21b52a3-NormAct-");
        ids.add("ms__109b2be1-d72a-4130-a43e-4eebdd7e586c-Activity");
        ids.add("ms__217a37ee-81bb-4a2d-ba7d-2d016e24cc80-Activity");
        ids.add("ms__9db64ec1-2ec6-482c-a2f1-5f7b12d2fb3b-WeekOffs");
        ids.add("ms__d2e70a47-8524-4bc8-a623-b7b14321e43d-Lead_146");
        ids.add("ms__fdfd9590-9a51-4a0a-876d-f5b6a336d2aa-Lead_146");
        ids.add("ms__30d5b2d5-dd9e-47fd-9e70-00c4bcb073b8-ICIMS-Em");
        ids.add("ms__4eb48c40-7913-4a98-b4d7-91628e4288df-ICIMS-Em");
        ids.add("ms__373f663f-511c-4dfc-910f-9e57138d412c-ICIMS-Ma");
        ids.add("ms__c025183d-d710-4c54-8727-0be14b4c52e0-ICIMS-Ma");
        ids.add("ms__cfe288b2-218f-4038-84cc-810ef2906ed6-ICIMS-En");
        ids.add("ms__97474009-bb41-4e8f-a230-008c9382705b-Alpha-Ac");
        ids.add("ms__a520aacf-9e6a-40dc-8a75-6492d5921ead-Initial");
        ids.add("ms__34c7c1ef-2c40-4a18-a445-64a20b7fdc85-Refine-R");
        ids.add("ms__37754647-adb7-4b48-95fe-1d3816011a1e-Refine-R");
        ids.add("ms__66634036-4a64-4d69-a40d-991e07f9d18a-Alpha-Ac");
        ids.add("ms__3b5d73da-b255-49d9-9187-5e1441903249-Lead_146");
        ids.add("ms__d3ad925d-0c19-49b5-bef1-fe60ee190da6-Lead_146");
        ids.add("ms__20961cac-61a8-4ede-98ee-3c6cc4d48e09-Lead_146");
        ids.add("ms__93c78e19-1cc2-4cdb-8ff5-c3a063847134-Lead_146");
        ids.add("ms__4322c773-e1dc-469e-b07c-d2fa0f3eec21-ysong2");
        ids.add("ms__2a8a6621-4254-4da8-b7c1-e6761083f62f-EMEA-Ini");
        ids.add("ms__ca6416eb-52a5-4e63-a76d-b76c88d9ddf0-EMEA-Ref");
        ids.add("ms__c74bd685-c108-40b9-ac75-fad589fedd89-APAC-Ini");
        ids.add("ms__4c439e42-9431-4f41-bdd5-b87842645730-APAC-Ref");
        ids.add("ms__715f1d55-eb85-4425-a4b2-5c355b5c26e2-Lead_146");
        ids.add("ms__60ae8d87-012e-4724-9e34-effc1faad6ee-APACUK-I");
        ids.add("ms__62f4137e-1c1b-42b1-b22f-bf3c2b50353b-APACUK-R");
        ids.add("ms__47b2d9b7-6100-4020-9e44-177a5424e19a-Telogis-");
        ids.add("ms__7ab38d53-3a6e-4168-8ece-944b4c281796-Telogis-");
        ids.add("ms__7c3a9cc7-f8a3-4034-bca4-023e8d7a1b33-Feng-Tes");
        ids.add("ms__3b72ee3f-ff63-4574-9cca-f0be89f02aa8-LP3-Corn");
        ids.add("ms__92480fb9-1d04-4953-826a-9cb6af1db8b7-Refined-");
        ids.add("ms__aececd31-e897-4d9d-8002-b4f8d9194ea7-Refined-");
        ids.add("ms__7b950fbc-0d64-49fd-bd2a-58ebc4a61bf4-Polycom-");
        ids.add("ms__e0f1501c-3473-40d6-a057-05b626d98829-Polycom-");
        ids.add("ms__fe743d5e-93eb-417b-938f-39bc5fe14257-Polycom-");
        ids.add("ms__0358bf39-42b9-4ca8-80d6-dce0cde28aca-Telogis-");
        ids.add("ms__a7de8c47-fdce-4b83-998e-dabd684825e2-Lattice-");
        ids.add("ms__480c3198-7a0c-4801-bf99-9da3cb93dc8b-US-Enter");
        ids.add("ms__ec87f363-8591-4316-a2f1-c718949676a8-Polycom-");
        ids.add("ms__27b4aedc-f74d-4c43-821d-61f8fc8bb57c-Polycom-");
        ids.add("ms__f62be444-3883-4b2a-b4ab-b971ed9968c0-Polycom-");
        ids.add("ms__e8deee34-6d70-4379-84fd-b85c2c4d06d2-Polycom-");
        ids.add("ms__0ad6e4f5-8ef7-4ef2-ae24-b826d39ea447-Polycom-");
        ids.add("ms__883e6eb2-b621-4c1a-8f20-34eb3165c1bf-Telogis-");
        ids.add("ms__912bc96d-a222-47ca-8e67-397f446b7ff4-Veracode");
        ids.add("ms__79a3db50-15c2-4005-b72c-2052656fcba0-Telogis-");
        ids.add("ms__a11f4e74-ab13-404c-99f4-3cee49600a79-Veracode");
        ids.add("ms__8380d4e5-7474-40fc-966a-30a7490596bb-Lattice-");
        ids.add("ms__81636fb2-56eb-402c-ac6e-088a5249432b-Tektroni");
        ids.add("ms__93cd0eb6-d228-491a-a74d-983daf0a869f-Lattice-");
        ids.add("ms__ba8206c1-2eae-4d53-9a15-25095092e4db-Tektroni");
        ids.add("ms__4b8012fb-4d17-4809-a07e-51cf96a6409c-US-Won-A");
        ids.add("ms__a7f6ceab-aaa3-4d0b-b586-e08503d536c1-Tektroni");
        ids.add("ms__546cf7e3-177a-4d55-9a83-afcb28826acf-US-Won-A");
        ids.add("ms__269387f5-2392-4535-87c5-90e56ae2c75b-US-Won-A");
        ids.add("ms__5b580b49-8ed8-423f-8cee-d73f84144099-Tektroni");
        ids.add("ms__be7ff758-c3e9-42a5-951c-3e322a82e954-Tektroni");
        ids.add("ms__be7ff758-c3e9-42a5-951c-3e322a82e954-Tektroni-001");
        ids.add("ms__55000eba-add3-4eee-8b3f-c0c93207c781-Hosting");
        ids.add("ms__fd04f858-179b-4a48-b45c-025b68ee561b-Training");
        ids.add("ms__b6aa4d5a-6e03-4a87-a38e-1a1148e04a74-Tektroni");
        ids.add("ms__3a6c12f1-82c2-45ed-b7f8-978a114d48e8-Lead_146");
        ids.add("ms__53b0b3bb-8e6d-4e50-a39f-b6b2bfef3507-Lattice-");
        ids.add("ms__e1fb416e-45eb-4af5-b3a5-72377384b833-Tektroni");
        ids.add("ms__863ebd85-87b6-4379-a5da-f08bb0919ca3-Tektroni");
        ids.add("ms__d0b16ceb-8088-4818-9270-119e2e8ad695-Tektroni");
        ids.add("ms__dcd65653-8ff1-4be7-94ee-fcf1309ab943-Tektroni");
        ids.add("ms__06a47440-f612-45a9-a754-fecb92e1425f-Tektroni");
        ids.add("ms__06a47440-f612-45a9-a754-fecb92e1425f-Tektroni-001");
        ids.add("ms__036d179b-8084-44b1-8e7c-fad373fbc315-US-Won-A");
        ids.add("ms__021bbbb2-10c0-46bb-9aa4-34cfd3d1bd4a-Canada-T");
        ids.add("ms__cd3b499f-e6ae-493d-9c97-f0ad62b4f8cb-US-Won-A");
        ids.add("ms__80c45cc4-313e-4fda-86a2-c96e47d22397-Account-");
        ids.add("ms__04557cc4-fb11-4a52-8e97-8b84ba7c3989-Account-");
        ids.add("ms__f9168130-f59c-46ee-bee9-18360f6d944a-US-Lead-");
        ids.add("ms__358705c6-cd3d-484d-a260-7fdd1c343552-EDW-Oppo");
        ids.add("ms__3eeda460-f375-4613-850c-36f4f2cefeaa-Veracode");
        ids.add("ms__fcf0d3ef-0e3d-442c-bfb1-5ea0743a41c3-EDW-Oppo");
        ids.add("ms__7f7bb060-67b7-48fb-b0e8-1dc89134691a-EDW-Warm");
        ids.add("ms__7cca13c0-f2d7-40ef-adc7-455996daba86-Lead_146");
        ids.add("ms__ea038d58-47b5-4998-bb8d-21310558d7c8-ysong3");
        ids.add("ms__40a68c75-5eba-49b5-9076-ca39caeb2073-EDW-Warm");
        ids.add("ms__257c0b1d-d463-4877-b60a-1b20fc5f7088-EDW-Warm");
        ids.add("ms__dca398eb-25e2-4268-8cde-9cdb1bb3ceb6-EDW-Acti");
        ids.add("ms__7d7fe57a-3e23-4adf-bf0f-571809ba0cc6-Hootsuit");
        ids.add("ms__fdcf5819-4551-4372-858c-76b1b26a5892-EDW-Acti");
        ids.add("ms__b4aabf0e-ebc8-4ace-b054-7158e4ae3a8d-OpenDNS-");
        ids.add("ms__f1f34325-65b2-4b50-a9b0-c701ee4f2e30-OpenDNS-");
        ids.add("ms__52659be7-ff48-4498-ae13-ed88d8e88a7d-OpenDNS-");
        ids.add("ms__f2e2b96f-03a6-4497-8e7d-9c441fc5aec6-OpenDNS-");
        ids.add("ms__63601e37-f7aa-4129-9b48-672c2122128a-OpenDNS-");
        ids.add("ms__85c57961-ffb6-48e4-b656-1e9262c8de20-NA-Initi");
        ids.add("ms__d884118e-2632-45b8-b327-fcc602d13e30-NA-Refin");
        ids.add("ms__b66ceae3-14f7-45e8-b0a3-f99c67630fe2-NA-Refin");
        ids.add("ms__aed2719f-fe2e-4eff-a4fd-b6828abb8046-Alpha-NA");
        ids.add("ms__3c1e811b-c71c-4faf-a061-1fa174784368-OpenDNS-");
        ids.add("ms__12070413-3743-4be9-bcb9-34cd8ad720b7-OpenDNS-");
        ids.add("ms__08c06a3d-2b4c-4ce7-a54b-b5a9a1b65f10-OpenDNS-");
        ids.add("ms__387cf358-ced6-46ba-aa17-ac7e6d8e513c-US-Enter");
        ids.add("ms__94ce8b64-2aff-434d-a571-0267827c6754-US-Enter");
        ids.add("ms__39655050-554a-4e21-85be-5cf8af827663-OpenDNS-");
        ids.add("ms__7ac79875-42b2-4923-ac54-d02484229ed2-US-Enter");
        ids.add("ms__5d6f57d7-9f3a-4468-a7a5-81c1da931459-Telogis-");
        ids.add("ms__8ff0148d-8299-4b5d-a8af-bd68e9c6bdf0-Telogis-");
        ids.add("ms__9616887e-48f7-4d15-ada8-1bc56f382e8f-Lattice-");
        ids.add("ms__9d5e7136-3c51-43a7-907a-bcbd4f8aa3cc-Lead_146");
        ids.add("ms__ca458209-0d5f-4449-8f83-354905c91941-300T50Sn");
        ids.add("ms__9c16d4ed-13dd-40ce-b2d6-cf4bac19f7a5-Mulesoft");
        ids.add("ms__07034038-9d25-4693-8d7c-b2f6827b7921-Warm-Acc");
        ids.add("ms__31b78ff8-ccf4-4fb6-8281-9af33fe7bac0-Hosting-");
        ids.add("ms__481295ba-d197-4503-84f0-6d9d55a180e0-Cold-Acc");
        ids.add("ms__a0e251cc-4cee-4879-aa36-507db0295d5f-Hosting-");
        ids.add("ms__bd571986-00df-43a1-a877-977cd3456bc3-Intuit-M");
        ids.add("ms__7023b289-c0d1-4558-ba76-948d1795af44-US-Midma");
        ids.add("ms__c51faa94-955e-42c8-8225-b8ebe82b1341-Tektroni");
        ids.add("ms__375e3319-1cfa-4686-8a23-5b306b486ebe-Lattice-");
        ids.add("ms__da1cc119-d17d-46f6-8abd-0de59952c7e2-Lattice-");
        ids.add("ms__f6891b03-091a-4cb1-8e1e-d4a00e36632d-Lattice-");
        ids.add("ms__53a197ef-1c17-4b02-88f7-528dc4778ea8-300T50Sn");
        ids.add("ms__87d8a241-db02-487b-b054-bf7033f3cd72-Lead_146");
        ids.add("ms__26ecf412-0a23-4338-bbaf-07fa72f2bac2-Demo-Lea");
        ids.add("ms__14e1c495-4a96-496a-bbaa-9dd4d373015a-GT1MM-US");
        ids.add("ms__a1029dfb-745c-4c96-8682-c926f465bd17-Mid-mark");
        ids.add("ms__7ca8f651-ecaa-45ff-8684-ad69bb986e8a-GT100K-1");
        ids.add("ms__9d574a09-62c7-4739-b589-de4132b3fe3d-US-GT1MM");
        ids.add("ms__3840f100-b69e-41ee-8b33-a7779c884576-US-GT100");
        ids.add("ms__355732cc-c744-4e2f-b4f5-d6232817020d-Enterpri");
        ids.add("ms__83f42592-e044-460d-b137-d3675bfec20f-Test1-20");
        ids.add("ms__ddb989be-d930-4e93-aaaf-660146be094b-Qlik-Mig");
        ids.add("ms__48487c2b-da6d-48e3-ae1a-e864193eca3e-Demo-Lea");
        ids.add("ms__ad388948-49a6-4020-9ce3-6c4fc289effe-Hosting-");
        ids.add("ms__6c0a6e4d-b709-462f-8e47-d6b7f5c1b36d-Telogis-");
        ids.add("ms__9b85cd56-43d3-40c8-a9f7-06f05f71dd86-Demo-Lea");
        ids.add("ms__ca6a13cf-e2cf-46cc-896c-938902469f97-Sample-D");
        ids.add("ms__50c846d1-19ce-4675-92d1-32606e4c89cf-Demo-Mod");
        ids.add("ms__4776c0ab-bccd-4077-b646-c509fa2b19bb-Sample-D");
        ids.add("ms__1ee7bc57-50e3-438c-ad99-ed010b040197-Lattice-");
        ids.add("ms__4338d4dc-0a09-4f8e-90d1-8d342da66ee2-Lattice-");
        ids.add("ms__bb56b6a0-6f9a-4cd3-8445-fdeb5b2f10cf-Intuit--");
        ids.add("ms__7e3b592f-63cc-4ce8-b8b9-0921a456d842-LeadMode");
        ids.add("ms__3d5588aa-a3a8-446c-9774-519219d83b0b-US-Lead-");
        ids.add("ms__959777cf-b366-40dd-a78d-93a24c80c1ef-Sample-D");
        ids.add("ms__a89614c8-195a-4969-9438-206955663829-Showpad-");
        ids.add("ms__f5cdc4c3-8cad-46b8-b5ae-9f22400f2224-OpenDNS-");
        ids.add("ms__72fa972e-d15c-412a-ac9b-01912c42f741-OpenDNS-");
        ids.add("ms__985b3360-dfae-41ad-ab29-1af52261edbe-OpenDNS-");
        ids.add("ms__7d8dc565-8a90-41ea-9df2-db4b2f37e7d5-OpenDNS-");
        ids.add("ms__a2387e6a-0679-402d-834f-5cf356605615-OpenDNS-");
        ids.add("ms__a2387e6a-0679-402d-834f-5cf356605615-OpenDNS--001");
        ids.add("ms__cf4b09bb-5e0d-418e-a758-7adaeb7d486a-Showpad-");
        ids.add("ms__2d8fcd48-844c-4d3c-99f0-8138fc65d909-Qlik-NA-");
        ids.add("ms__2349e4f1-c577-4c53-8c64-5db62f6026c4-Showpad-");
        ids.add("ms__e6dbe252-cdcb-40ed-9619-a21dda8d602b-LiveRamp");
        ids.add("ms__d22fcb7f-acba-4aae-a9ec-333fe9d717ff-Cold-Acc");
        ids.add("ms__c019b461-980b-444b-8088-ca11b022af6b-Sample-D");
        ids.add("ms__e599dd0b-3172-4a83-843d-8e4ec76368f7-Cold-Acc");
        ids.add("ms__70c87f81-11ed-4134-a157-1b64fae6178c-Initial");
        ids.add("ms__8eadf094-212e-4c4c-b236-d540360014c2-Cloud-NA");
        ids.add("ms__1b6bfb08-fce3-4b88-b2ef-53f375d2901c-Cloud-In");
        ids.add("ms__5eba723a-69df-4d92-83ee-17647ada2411-Initial");
        ids.add("ms__3dd972d9-d683-428b-adbc-2179a8aa569d-Initial");
        ids.add("ms__40caad68-ba0b-499e-9847-59d4e81a1ff5-Initial");
        ids.add("ms__f50fd05f-2a31-4b93-8be2-a4eac21b4e3f-Initial");
        ids.add("ms__befbbe06-6cd0-42b1-8761-5a1b4e4eeb02-Sample-D");
        ids.add("ms__5937ec50-4aa7-45bc-a6dd-48646136378e-Lattice-");
        ids.add("ms__e992a2a1-b051-4562-9f64-30e022c53c78-NA-Initi");
        ids.add("ms__71b153a8-6452-4a91-b66d-0b849d1238a8-EMEA-Ini");
        ids.add("ms__40746716-f9e7-4d18-bfe4-346098a331f3-LiveRamp");
        ids.add("ms__15534a09-7d6c-4ec5-bd96-daebe460eaa5-Direct-N");
        ids.add("ms__ab51f943-c2d8-431e-bd96-eebf748a01c4-OpenDNS-");
        ids.add("ms__e1bb1721-8bec-4288-9150-30792e5481fe-Mulesoft");
        ids.add("ms__055fb515-d206-492f-888f-6f45f3be940e-Direct-I");
        ids.add("ms__39720f41-be73-4f5e-a6ac-78cecd23f66d-OpenDNS-");
        ids.add("ms__30a21f31-4eaa-4750-bdf2-ff2399678869-Dell-Boo");
        ids.add("ms__9f31a760-1639-4866-a501-61f941184e32-Showpad-");
        ids.add("ms__622fa6da-97ac-4b68-919c-8eebbdf01c03-Intuit-D");
        ids.add("ms__0c2c9ad0-432c-4c95-84d5-d150fda33b76-Mulesoft");
        ids.add("ms__e3f99b37-60e4-4e0d-a216-6c67e14f71c2-HootSuit");
        ids.add("ms__5c00d2ee-01fc-4585-9bee-7b967e46e7d8-Cornerst");
        ids.add("ms__044a43d9-ea74-422b-80dd-c569c0d2c203-ClosedWo");
        ids.add("ms__c416da29-e139-4ee2-b9b3-b4b5e1742cb6-Negotiat");
        ids.add("ms__fe55308e-a4c8-41e4-a447-a72e118c5e6c-Negotiat");
        ids.add("ms__8c02dee3-25f1-45c1-8f2b-7e162ae30409-Dell-Boo");
        ids.add("ms__e07526bf-0e05-4e72-9f4d-23763ed3c1f2-Dell-Boo");
        ids.add("ms__478022f6-dec3-47a1-9a13-82f2d6cfdd98-Copy-of-");
        ids.add("ms__e335dec2-c21d-4a89-90db-b07462d14376-POC-1-0");
        ids.add("ms__1263337f-357d-421a-a6ee-4d33c88d5c5d-Copy-of-");
        ids.add("ms__11b46d2c-f0d2-4791-b84d-4bff0716a2b1-POCInput");
        ids.add("ms__6d260d6f-33c8-492a-aee6-5b9256180930-POC-Test");
        ids.add("ms__719d3283-51fe-4014-b3bc-f2e37e307989-POCInput");
        ids.add("ms__e91dcca4-a35c-4afe-91e8-a8ed925ed696-Veracode");
        ids.add("ms__7657b54e-51bc-45fb-a694-d572725d65d0-test-Pat");
        ids.add("ms__c8ef2b5b-ad92-4365-afa8-53d555b9e013-300T50Sn");
        ids.add("ms__b113f065-0550-4b4f-8d00-ee380a08f78b-Veracode");
        ids.add("ms__3066676a-957b-4f9e-9d2c-8f50d5257f00-Sage-Lea");
        ids.add("ms__7a4eaa5e-b27b-4a33-94c5-9758a30d82b0-Sage-Lea");
        ids.add("ms__9dc26556-6aa6-4293-92c7-fcba3a3186c3-Sage-Lea");
        ids.add("ms__b3433af7-73af-4e39-a4d0-68c289afb361-Sage-Lea");
        ids.add("ms__328aa002-afb2-4a72-ba29-42671800e8fc-Dell-Boo");
        ids.add("ms__085bf590-a5d8-4ca1-83d7-dd62494b7ccc-File1-Ho");
        ids.add("ms__12873212-912c-4f6a-9c4c-1f7619075e23-Lattice-");
        ids.add("ms__234ba872-4c31-4002-a595-df207038ec6b-Mulesoft");
        ids.add("ms__a0127db0-6d8d-4846-a75b-fe958fa7f3dd-Lead_146");
        ids.add("ms__a6473ecb-2e0a-41e1-9ecc-87b62af35639-NA-20160");
        ids.add("ms__aeca6877-22e1-4f06-b17c-51c70f2e4927-Lead_146");
        ids.add("ms__73014591-d442-4980-bb05-bf4195d374f4-Lattice-");
        ids.add("ms__fb674fa1-658d-4ba4-a091-e050a5f77dd0-Lead_146");
        ids.add("ms__3b2a9207-7d67-4e73-b5b3-95fa28c88c78-EMEA-201");
        ids.add("ms__681a887a-2301-4652-9262-544021412e14-OpenDNS-");
        ids.add("ms__ef8e4ea2-2bac-4ad7-8941-97ed77f898f8-OpenDNS-");
        ids.add("ms__63ec2324-3401-4aee-b658-5d31419b77b5-OpenDNS-");
        ids.add("ms__19c228a1-5c9b-4459-9d8d-ea347471e233-OpenDNS-");
        ids.add("ms__6b4b4566-6682-4f85-9de0-0f930b8833ae-Showpad-");
        ids.add("ms__b3d60717-57ca-4acc-8e7a-c77d8c1b4cfa-OpenDNS-");
        ids.add("ms__772ede5c-1740-45cd-8a72-76bc3cb7386c-Showpad-");
        ids.add("ms__4f799e6b-4003-4aaa-87c2-d813e613956b-OpenDNS-");
        ids.add("ms__aec49a37-3a4a-4a03-88f2-7fa41e3dc3b1-testMode");
        ids.add("ms__4322f733-3419-41a9-b569-305555859a51-Account-");
        ids.add("ms__e312f5c0-4a7c-41f9-a58e-5f63c2cae5b6-LiveRamp");
        ids.add("ms__2c4395ef-80ce-445c-a33c-07f84ae70516-LiveRamp");
        ids.add("ms__51d9f23c-fd44-4173-825f-592e00c560d7-OpenDNS-");
        ids.add("ms__07c87108-3e8d-44ce-a6b0-728e3e99afc1-OpenDNS-");
        ids.add("ms__f7b5549b-e70b-413b-8253-f0b8752b23a5-OpenDNS-");
        ids.add("ms__01b30295-adb1-485f-b994-34310ac3fc94-OpenDNS-");
        ids.add("ms__f27dea95-c53d-452b-9f52-97207afbc93b-OpenDNS-");
        ids.add("ms__491c5537-c4a9-4fb1-94dc-c775dbe0633f-OpenDNS-");
        ids.add("ms__2e31500e-7663-4860-b0f8-2f715a985512-OpenDNS-");
        ids.add("ms__09c8a7e7-3d70-4fc9-9d78-6068b3250454-LiveRamp");
        ids.add("ms__34d61329-7a0c-4e27-8c76-e56974b108de-OpenDNS-");
        ids.add("ms__6f777523-adc4-4714-9073-30959f82e5ad-OpenDNS-");
        ids.add("ms__d16f2718-8baf-4a76-96cd-0aaa69da8a7b-OpenDNS-");
        ids.add("ms__952aa3c7-37be-40a4-bb17-148495d2f364-Liveramp");
        ids.add("ms__66669f68-a952-4abb-ab52-fb06f9d496f8-Lattice-");
        ids.add("ms__84e80c52-8ae6-480d-b604-61afbcd45f3e-Simon-La");
        ids.add("ms__15e6ac1f-dea6-422e-9ab0-e1573336849e-Hootsuit");
        ids.add("ms__3fa14c59-d455-4b6f-b341-01e80ef8356a-CitrixSa");
        ids.add("ms__843020e4-c0b3-45e4-a55a-6adeeb90a2bb-SFDCAcco");
        ids.add("ms__c76aa028-4606-4d78-953d-6f6a87127990-SFDCLead");
        ids.add("ms__3ce68685-4b96-4239-8120-04c8dda4eb8a-OpenDNS-");
        ids.add("ms__13a0abd1-4f9c-4529-95dd-bee7350a1825-SFDCAcco");
        ids.add("ms__2382b485-35bb-4294-a520-4d153c16f2c3-Activity");
        ids.add("ms__264807b6-31ad-4a21-9bfa-9c443304f06f-Jeff-Sim");
        ids.add("ms__6acb7cb6-b9c2-44b6-aeed-a72b492a99c4-Activity");
        ids.add("ms__88931475-d1c8-4862-9342-a32704d86ca0-US-Enter");
        ids.add("ms__a0fb9db2-9554-4f72-b716-8bb32d2ec382-Sample-D");
        ids.add("ms__85eecbd4-4069-479d-8567-70b7ebf70677-Mulesoft");
        ids.add("ms__1ff99f09-a749-45e0-add2-6b5790e4ef67-Lattice-");
        ids.add("ms__fa950ce0-9dfd-42b9-8077-8644588c4af5-Hootsuit");
        ids.add("ms__ff8a7a5c-c563-47d7-988c-5dc83cce0294-SageLive");
        ids.add("ms__48969f3a-c285-4e5d-bcdd-7d98edb4ce6d-OpenDNS-");
        ids.add("ms__a631a19c-bb14-45c8-9c5e-afeec0537f2f-OpenDNS-");
        ids.add("ms__650bb3ff-4b4a-4065-bea4-f505c46ffc9d-ODP-LP3-");
        ids.add("ms__f6782ce4-5aa6-487c-bdff-68cc5ff936f9-ODP-LP3-");
        ids.add("ms__9b303afd-ec3a-4761-bc85-801e6230bccc-Individu");
        ids.add("ms__673c48d2-98a4-4369-accc-fd4fb7a1785a-Lead_146");
        ids.add("ms__272db925-88b2-407b-8f0d-bbc87776b319-300T50Sn");
        ids.add("ms__5b1526c2-f1a3-4c83-b2e3-486f21e8aaf1-Lattice-");
        ids.add("ms__985c9828-5e14-48ae-8b62-d23b98903ecd-CitrixSa");
        ids.add("ms__ee68f830-7e55-4544-98b2-2c840bb5bf8c-SFDCAcco");
        ids.add("ms__efb240d0-bce7-4036-a008-9fd66385ba79-CA-GT100");
        ids.add("ms__f972f5d7-6e42-4787-99bf-fc9b31ec0452-US-Enter");
        ids.add("ms__510c3ac0-65f9-49cb-a4a4-6fd221effc54-CA-GT250");
        ids.add("ms__46fd9924-43e5-4e8e-aaca-6b84bce591df-Reputati");
        ids.add("ms__8eec188d-83a1-40ea-b11a-9913a4b18af1-Lead-Sco");
        ids.add("ms__1cb6de49-46df-4372-87fa-8b8f848c88b2-CSOD-Acc");
        ids.add("ms__ecda3729-0397-49bc-9c48-2fb988c2f2fe-CSOD-Acc");
        ids.add("ms__dec08dd4-3d15-48a6-885b-35ee2cac20d8-Lattice-");
        ids.add("ms__e3a5e89d-c59e-4e2a-920d-dfe113b36be8-Lattice-");
        ids.add("ms__1092e444-e0cb-40a6-ad44-274f268f729c-Reputati");
        ids.add("ms__14b55960-8878-4329-99dd-ed253e097454-qbr-prac");
        ids.add("ms__d8bc1983-d206-4554-9e5a-9c73d8eefabd-Sample-D");
        ids.add("ms__5aa026c5-a436-4a70-b4ac-1fce4cab66d2-Reputati");
        ids.add("ms__6f929310-bb3a-43a7-818a-c35f423d0575-Internal");
        ids.add("ms__84eaf3ef-e9dc-45b5-9725-3bed3963596f-Slater-A");
        ids.add("ms__6aea863e-0b57-4f92-a7c5-4af905b81db1-Demo-Tra");
        ids.add("ms__46518053-58b8-4596-a600-90435e37acf5-Training");
        ids.add("ms__6280aff2-2895-4f79-b144-5900498d2f4e-singh-20");
        ids.add("ms__e9390cc6-1764-4829-b694-4a4da8b2481e-QBR-trai");
        ids.add("ms__d63f01f1-1af5-4d18-b653-ccb568c750ba-Ravi-s-M");
        ids.add("ms__0a2a9749-2eb8-4964-8edb-61f6e5531c7a-Internal");
        ids.add("ms__6fbf49ce-cd41-461e-a4d6-1eb5a44e2ac9-Internal");
        ids.add("ms__c0971b26-96e6-46af-86cc-441a3a78ca20-Ravi-s-M");
        ids.add("ms__c766b587-d3be-4fb4-bff1-a7da429cfd49-Veracode");
        ids.add("ms__3029bc75-71f8-4cb2-872b-c28d2f71ef7b-Veracode");
        ids.add("ms__f5b6e895-1845-47a9-beef-4df546efb7c9-US-Enter");
        ids.add("ms__dc0be438-be67-4668-b2de-ff1a6d5675b6-Internal");
        ids.add("ms__8b483188-5443-4726-97c5-834051c11f6a-Cornerst");
        ids.add("ms__9d33c945-a361-4c43-acaa-a7948afd9572-Lattice-");
        ids.add("ms__24ab7ab1-b264-4c42-b222-c46226331145-File1-Ho");
        ids.add("ms__7e9d8e34-9740-4b67-8782-cfc88d7b7e33-Individu");
        ids.add("ms__68a15374-d1bf-428a-bc25-6aa772347648-Individu");
        ids.add("ms__cd3552b2-e469-4539-a23b-9e94d7b536b0-example-");
        ids.add("ms__4bb24a13-17be-49f6-ba16-170dbee3eec7-CA-GT250");
        ids.add("ms__368f9817-310b-4594-81a6-24acc58d9b8a-CA-GT100");
        ids.add("ms__20d7b521-fca4-450a-a724-0c52e3b9f3d0-PayPal-C");
        ids.add("ms__093181f8-c287-460c-ab6e-a1a832149ccc-250ca");
        ids.add("ms__5c3eeaf1-9c0a-4a1b-b092-c17c558491d7-250knewi");
        ids.add("ms__61738c28-fb43-44da-a564-8a1c929cf544-PayPal-C");
        ids.add("ms__1a4b24d2-7eb2-4ffa-b9ea-828b45ca1d58-PayPal-C");
        ids.add("ms__3eb0ba41-aa2c-4813-9452-3a2c332d6cc8-LiveRamp");
        ids.add("ms__53a42598-3601-4e34-9bf4-62e60a9c30da-Showpad-");
        ids.add("ms__4c51834e-028f-4796-863e-10be4f14eebb-BAMS-Con");
        ids.add("ms__38716fef-5db0-4757-a670-92f061c63e75-Test-Mod");
        ids.add("ms__4a5396b3-7def-4625-b147-3834bb0fd5c2-OpenDNS-");
        ids.add("ms__3e01587e-d8f9-4f56-8c58-f83848343c0f-CSOD-Ful");
        ids.add("ms__4af45373-edca-410a-b9bf-3aef36d1984f-CSOD-Min");
        ids.add("ms__a7fb67e0-57a4-4db8-8fd7-ffaba4e8c5f4-CSOD-min");
        ids.add("ms__749cd58b-9f44-4e65-89f4-8068c99f71a1-Test-Acc");
        ids.add("ms__29e13b64-a917-4359-bfa5-ad8e594a6608-LRAllClo");
        ids.add("ms__20be0d06-0f01-4a2e-8365-14bb4ad893db-LRClosed");
        ids.add("ms__b44116ca-605d-4f5c-bfeb-8d2a929dc8f3-LRallcon");
        ids.add("ms__010bf2a2-d4a2-4ab9-834e-3868ca519776-Test-Mod");
        ids.add("ms__aad1f7a3-9a2c-4469-88ae-210467f27fe8-LRminusl");
        ids.add("ms__967b28f8-21b7-499f-be26-f525e9c25b04-OpenDNS-");
        ids.add("ms__2cf048bb-aaf0-442e-afb6-f4e12b43c616-CSOD-min");
        ids.add("ms__042a2c96-8431-4d00-aa2f-1ec1338f0d54-CSOD-exp");
        ids.add("ms__d02b4318-06a9-452d-b656-dcd7b2bcda1a-OpenDNS-");
        ids.add("ms__7d52ab92-0181-4903-8dd4-e67cfcacfe6e-CSOD-exp");
        ids.add("ms__8611a65f-1c91-4753-9641-0c7716596555-LRWONPRO");
        ids.add("ms__a3fb4469-d296-4037-b1c5-4a62a61c3b0e-LRwonneg");
        ids.add("ms__1a996842-f6e9-4ee5-9640-bc5d2713e1fb-Harralso");
        ids.add("ms__653be91e-647e-40bd-a674-7833a4f20408-LRnolost");
        ids.add("ms__baec9e53-7bcb-456b-86e8-15909cc98aa7-LRnointr");
        ids.add("ms__0c9df859-5996-4949-be41-a2dd8f109286-CSOD-Dyl");
        ids.add("ms__bea4f39d-9ea3-47d4-9674-aa9daaaac95a-CSOD-inc");
        ids.add("ms__d81d7d05-0aa0-458d-bf13-c15cd279d7f9-Harralso");
        ids.add("ms__c7cb0b19-194d-4b89-bc30-fe261164fbc8--TEST--E");
        ids.add("ms__510a5323-8d30-4acf-b44e-89b1294ca395--TEST--E");
        ids.add("ms__f8b0e07f-d1cd-4d70-a755-31d999199366-Harralso");
        ids.add("ms__d7e818b7-f1fa-4bda-8447-717d1d9e939d--TEST--E");
        ids.add("ms__70304097-9383-498a-81df-e41415ebc33b-training");
        ids.add("ms__837facf8-8c32-4a5f-ad69-cde7d2825075-training");

        final Deque<Entry> entries = new ConcurrentLinkedDeque<>();

        Parallel.foreach(ids, new Parallel.Operation<String>() {
            @Override
            public void perform(String id) {
                System.out.println("On model id " + id);
                ModelSummary summary = modelSummaryEntityMgr.getByModelId(id);

                if (!StringUtils.isEmpty(summary.getTrainingTableName())
                        && summary.getTrainingTableName().contains("Source")) {
                    Table table = metadataProxy.getTable(summary.getTenant().getId(), summary.getTrainingTableName());
                    summary.setPredictors(null);
                    summary.setModelSummaryProvenanceProperties(null);
                    if (table != null) {
                        table.setTenant(summary.getTenant());

                        MultiTenantContext.setTenant(summary.getTenant());
                        SourceFile sourceFile = sourceFileService.findByTableName(table.getName());
                        if (sourceFile == null) {
                            entries.add(new Entry(table, summary));
                        }

                    }
                }
            }
        });

        return entries;
    }
}
