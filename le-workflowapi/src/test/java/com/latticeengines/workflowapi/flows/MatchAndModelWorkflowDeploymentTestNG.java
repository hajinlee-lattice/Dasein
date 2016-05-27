package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.MatchAndModelAndEmailWorkflow;
import com.latticeengines.leadprioritization.workflow.MatchAndModelWorkflowConfiguration;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.pls.workflow.MatchAndModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class MatchAndModelWorkflowDeploymentTestNG extends ImportMatchAndModelWorkflowDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(MatchAndModelWorkflowDeploymentTestNG.class);

    protected static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private ModelSummaryService modelSummaryService;

    private Table accountTable;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private MatchAndModelWorkflowSubmitter matchAndModelWorkflowSubmitter;

    @Autowired
    private MatchAndModelAndEmailWorkflow modelAndEmailWorkflow;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
        setupTables();
        setupModels();
    }

    private void setupTables() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "/tables/Account.json");
        accountTable = JsonUtils.deserialize(IOUtils.toString(ins), Table.class);
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), accountTable.getName(),
                accountTable);

        String path = getClass().getClassLoader().getResource(RESOURCE_BASE + "/tables/part-v001-o000-00000.avro")
                .getPath();
        Schema schema = AvroUtils.readSchemaFromLocalFile(path);
        Table eventTable = MetadataConverter.getTable(schema, new ArrayList<Extract>(), null, null);
        eventTable.setName("RunMatchWithLEUniverse_152722_DerivedColumnsCache_with_std_attrib");
        eventTable.getAttribute(InterfaceName.Website.name()).setTags(Tag.INTERNAL);
        eventTable.getAttribute(InterfaceName.City.name()).setTags(Tag.INTERNAL);
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), eventTable.getName(), eventTable);

        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/tables/SourceFile_Account_csv.avro");
        String parent = accountTable.getExtracts().get(0).getPath().replace("*.avro", "Account.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(), parent);
    }

    protected void setupModels() throws IOException {
        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/models/AccountModel");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(),
                "/user/s-analytics/customers/" + DEMO_CUSTOMERSPACE.toString()
                        + "/models/RunMatchWithLEUniverse_152637_DerivedColumnsCache_with_std_attrib/");
    }

    @Test(groups = "deployment", enabled = true)
    public void modelAccountData() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);

        log.info("wait 15 sec for model to be downloaded.");
        Thread.sleep(15000L);
        List<VdbMetadataField> metadata = modelMetadataService.getMetadata(summary.getId());
        for (VdbMetadataField field : metadata) {
            if (field.getColumnName().equals(InterfaceName.Website.name())) {
                field.setApprovedUsage(ApprovedUsage.NONE.toString());
            }
            if (field.getColumnName().equals(InterfaceName.City.name())) {
                field.setApprovedUsage(ApprovedUsage.NONE.toString());
            }
        }

        Table clone = modelMetadataService.cloneTrainingTable(summary.getId());
        ModelSummary modelSummary = modelSummaryService.getModelSummaryEnrichedByDetails(summary.getId());
        cloneAndRemodel(clone, modelMetadataService.getAttributesFromFields(clone.getAttributes(), metadata),
                modelSummary);

        summary = locateModelSummary("testWorkflowAccount_clone", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        metadata = modelMetadataService.getMetadata(summary.getId());
        for (VdbMetadataField field : metadata) {
            if (field.getColumnName().equals(InterfaceName.Website.name()) || field.getColumnName().equals(InterfaceName.City.name())) {
                assertEquals(field.getApprovedUsage(), ApprovedUsage.NONE.toString());
            }
        }
    }

    protected void cloneAndRemodel(Table clone, List<Attribute> userRefinedAttributes, ModelSummary modelSummary)
            throws Exception {
        MatchAndModelWorkflowConfiguration configuration = matchAndModelWorkflowSubmitter.generateConfiguration(
                clone.getName(), "testWorkflowAccount_clone", "clone", TransformationGroup.STANDARD,
                userRefinedAttributes, modelSummary);
        WorkflowExecutionId workflowId = workflowService.start(modelAndEmailWorkflow.name(), configuration);

        waitForCompletion(workflowId);
    }
}
