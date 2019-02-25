package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CleanUpEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CleanUpEndToEndDeploymentTestNG.class);

    private static final String TRAINING_CSV_FILE = "Lattice_Relaunch_Small.csv";

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    private String modelId;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceLead, TRAINING_CSV_FILE);
        log.info("model Id: " + modelId);
    }

    @Test(groups = "deployment.lp")
    public void cleanUpModel() throws Exception {
        ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
        assertNotNull(modelSummary);

        Tenant tenant = modelSummary.getTenant();
        assertNotNull(tenant);

        String eventTableName = modelSummary.getEventTableName();
        Table eventTable = metadataProxy.getTable(tenant.getId(), eventTableName);
        assertNotNull(eventTable);

        Extract extract = eventTable.getExtracts().get(0);
        assertNotNull(extract);

        String eventTableAvroFilePath = extract.getPath();
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, eventTableAvroFilePath));

        String modelSummarySupportingFilePath = String.format("/user/s-analytics/customers/%s/models/%s",
                tenant.getId(), modelSummary.getEventTableName());
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelSummarySupportingFilePath));

        ResponseDocument response = restTemplate.postForObject(String.format("%s/pls/models/cleanup/%s",
                getDeployedRestAPIHostPort(), modelId), null, ResponseDocument.class);
        assertTrue(response.isSuccess());

        assertFalse(HdfsUtils.fileExists(yarnConfiguration, eventTableAvroFilePath));
        assertFalse(HdfsUtils.fileExists(yarnConfiguration, modelSummarySupportingFilePath));

        modelSummary = modelSummaryProxy.getByModelId(modelId);
        assertNull(modelSummary);
    }
}
