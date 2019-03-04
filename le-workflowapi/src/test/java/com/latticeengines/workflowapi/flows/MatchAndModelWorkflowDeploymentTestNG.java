package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.MatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelNoteService;
import com.latticeengines.pls.workflow.MatchAndModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.lp.ModelCopyProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class MatchAndModelWorkflowDeploymentTestNG extends ImportMatchAndModelWorkflowDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchAndModelWorkflowDeploymentTestNG.class);

    protected static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    private Table accountTable;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private MatchAndModelWorkflowSubmitter matchAndModelWorkflowSubmitter;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    @Autowired
    private ModelNoteService modelNoteService;

    @Inject
    private ModelCopyProxy modelCopyProxy;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);
        setupTables();
        setupModels();
    }

    @SuppressWarnings("deprecation")
    private void setupTables() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "/tables/Account.json");
        accountTable = JsonUtils.deserialize(IOUtils.toString(ins), Table.class);
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), accountTable.getName(),
                accountTable);

        String path = getClass().getClassLoader().getResource(RESOURCE_BASE + "/tables/part-v001-o000-00000.avro")
                .getPath();
        Schema schema = AvroUtils.readSchemaFromLocalFile(path);
        Table eventTable = MetadataConverter.getTable(schema, new ArrayList<Extract>(), null, null, false);
        eventTable.setName("RunMatchWithLEUniverse_152722_DerivedColumnsCache_with_std_attrib");
        eventTable.getAttribute(InterfaceName.Website.name()).setTags(Tag.INTERNAL);
        eventTable.getAttribute(InterfaceName.City.name()).setTags(Tag.INTERNAL);
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), eventTable.getName(), eventTable);

        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/tables/SourceFile_Account_csv.avro");
        String parent = accountTable.getExtracts().get(0).getPath().replace("*.avro", "Account.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(), parent);
    }

    protected void setupModels() throws IOException {
        String uuid = UUID.randomUUID().toString();
        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/models/AccountModel/random_uuid");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(),
                "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                        + "/models/RunMatchWithLEUniverse_152637_DerivedColumnsCache_with_std_attrib/" + uuid);
        String summaryHdfsPath = "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                + "/models/RunMatchWithLEUniverse_152637_DerivedColumnsCache_with_std_attrib/" + uuid
                + "/enhancements/modelsummary.json";
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, summaryHdfsPath));

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(RESOURCE_BASE + "/models/AccountModel/random_uuid/enhancements/modelsummary.json");
        String summary = IOUtils.toString(is, Charset.forName("UTF-8"));
        summary = summary.replace("{% uuid %}", uuid);
        HdfsUtils.writeToFile(yarnConfiguration, summaryHdfsPath, summary);
    }

    @Test(groups = "workflow", enabled = true)
    public void modelAccountData() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", mainTestCustomerSpace);
        assertNotNull(summary);
        modelSummaryProxy.createModelSummary(mainTestCustomerSpace.toString(), summary, false);
        List<VdbMetadataField> metadata = modelMetadataService.getMetadata(summary.getId());
        for (VdbMetadataField field : metadata) {
            if (field.getColumnName().equals(InterfaceName.Website.name())) {
                field.setApprovedUsage(ApprovedUsage.NONE.toString());
            }
            if (field.getColumnName().equals(InterfaceName.City.name())) {
                field.setApprovedUsage(ApprovedUsage.NONE.toString());
            }
        }

        Table clone = modelCopyProxy.cloneTrainingTable(MultiTenantContext.getShortTenantId(), summary.getId());
        ModelSummary modelSummary = modelSummaryProxy.getModelSummaryEnrichedByDetails(MultiTenantContext.getTenant().getId(),
                summary.getId());
        cloneAndRemodel(clone, modelMetadataService.getAttributesFromFields(clone.getAttributes(), metadata),
                modelSummary);

        summary = locateModelSummary("testWorkflowAccount_clone", mainTestCustomerSpace);
        assertNotNull(summary);
        metadata = modelMetadataService.getMetadata(summary.getId());
        for (VdbMetadataField field : metadata) {
            if (field.getColumnName().equals(InterfaceName.Website.name())
                    || field.getColumnName().equals(InterfaceName.City.name())) {
                assertEquals(field.getApprovedUsage(), ApprovedUsage.NONE.toString());
            }
        }
        List<ModelNote> list = modelNoteService.getAllByModelSummaryId(summary.getId());
        assertEquals(list.size(), 2);
    }

    protected void cloneAndRemodel(Table clone, List<Attribute> userRefinedAttributes, ModelSummary modelSummary)
            throws Exception {
        CloneModelingParameters parameters = new CloneModelingParameters();
        parameters.setName("testWorkflowAccount_clone");
        parameters.setDisplayName("clone");
        parameters.setDeduplicationType(DedupType.MULTIPLELEADSPERDOMAIN);
        parameters.setExcludePropDataAttributes(Boolean.FALSE);
        parameters.setEnableTransformations(new Random().nextBoolean());
        parameters.setNotesContent("this is another test case");

        NoteParams noteParams = new NoteParams();
        noteParams.setUserName("penglong.liu@lattice-engines.com");
        noteParams.setContent("this is a test case");
        modelNoteService.create(modelSummary.getId(), noteParams);
        MatchAndModelWorkflowConfiguration workflowConfig = matchAndModelWorkflowSubmitter.generateConfiguration(
                clone.getName(), parameters, TransformationGroup.STANDARD, userRefinedAttributes, modelSummary);
        modelSummaryProxy.setDownloadFlag(MultiTenantContext.getTenant().getId());

        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);

        waitForCompletion(workflowId);
    }
}
