package com.latticeengines.cdl.workflow.steps.validations.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.CDLWorkflowFunctionalTestNGBase;
import com.latticeengines.cdl.workflow.steps.validations.service.impl.ProductFileValidationService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.pls.ProductValidationSummary;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ProductFileValidationConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

public class ProductFileValidationServiceFunctionalTestNG extends CDLWorkflowFunctionalTestNGBase {

    @Inject
    private ProductFileValidationService productFileValidationService;


    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        super.setup();
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        String hdfsDir = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        fileDestination = hdfsDir + "/product/";
        if (!HdfsUtils.fileExists(yarnConfiguration, fileDestination)) {
            HdfsUtils.mkdir(yarnConfiguration, fileDestination);
        }
        fileName = "product.avro";
        InputStream in = testArtifactService.readTestArtifactAsStream(TEST_AVRO_DIR, TEST_AVRO_VERSION, "Product1" +
                ".avro");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, in,  fileDestination + fileName);
    }

    @AfterClass(groups = {"functional"})
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, fileDestination);
    }

    @Override
    protected String getFlowBeanName() {
        return null;
    }

    @Test(groups = "functional")
    public void testProductFileValidations() {
        DataCollectionProxy dataCollectionProxy = Mockito.mock(DataCollectionProxy.class);
        RatingEngineProxy ratingEngineProxy = Mockito.mock(RatingEngineProxy.class);
        SegmentProxy segmentProxy = Mockito.mock(SegmentProxy.class);
        DataFeedProxy dataFeedProxy = Mockito.mock(DataFeedProxy.class);
        when(segmentProxy.getMetadataSegments(anyString())).thenReturn(null);
        when(ratingEngineProxy.getAllModels(anyString())).thenReturn(null);
        when(ratingEngineProxy.getRatingEngineSummaries(anyString())).thenReturn(new ArrayList<>());
        when(dataCollectionProxy.getActiveVersion(anyString())).thenReturn(DataCollection.Version.Blue);
        when(dataCollectionProxy.getTable(anyString(), any(TableRoleInCollection.class),
                any(DataCollection.Version.class))).thenReturn(null);
        when(dataFeedProxy.getDataFeedTask(anyString(), anyString())).thenReturn(null);
        ReflectionTestUtils.setField(productFileValidationService, "dataCollectionProxy", dataCollectionProxy);
        ReflectionTestUtils.setField(productFileValidationService, "ratingEngineProxy", ratingEngineProxy);
        ReflectionTestUtils.setField(productFileValidationService, "segmentProxy", segmentProxy);
        ReflectionTestUtils.setField(productFileValidationService, "dataFeedProxy", dataFeedProxy);

        ProductFileValidationConfiguration configuration = new ProductFileValidationConfiguration();
        configuration.setPathList(Collections.singletonList(fileDestination + fileName));
        configuration.setCustomerSpace(CustomerSpace.parse("test"));
        configuration.setDataFeedTaskId("test");
        List<String> processedRecords = Collections.singletonList("50");
        EntityValidationSummary summary = productFileValidationService.validate(configuration, processedRecords);
        Assert.assertTrue(summary instanceof ProductValidationSummary);
        ProductValidationSummary productSummary = (ProductValidationSummary) summary;
        Assert.assertTrue(productSummary.getAddedBundles().size() > 0);
        Assert.assertEquals(productSummary.getDifferentSKU(), 0);
        Assert.assertEquals(productSummary.getErrorLineNumber(), 0);
        Assert.assertEquals(productSummary.getMissingBundles().size(), 0);
        Assert.assertEquals(productSummary.getProcessedBundles().size(), 0);


    }
}
