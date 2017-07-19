package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.functionalframework.PropDataLeadEnrichmentAttributeServlet;
import com.latticeengines.pls.service.LeadEnrichmentService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.rest.StandaloneHttpServer;

public class LeadEnrichmentServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private LeadEnrichmentService leadEnrichmentService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    private StandaloneHttpServer httpServer;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        columnMetadataProxy.setHostport("http://localhost:8076");
        httpServer = new StandaloneHttpServer();
        httpServer.init(8076);
        httpServer.addServlet(new PropDataLeadEnrichmentAttributeServlet(),
                "/match/metadata/predefined/" + Predefined.LeadEnrichment);
        httpServer.start();
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        httpServer.stop();
    }

    @Test(groups = "functional")
    public void testGetAttributes() throws InterruptedException {
        List<LeadEnrichmentAttribute> attributes = leadEnrichmentService.getAvailableAttributes();
        Assert.assertEquals(attributes.size(), 2);
        LeadEnrichmentAttribute attribute = attributes.get(0);
        Assert.assertEquals(attribute.getFieldName(), "TechIndicator_AddThis");
        Assert.assertEquals(attribute.getFieldType(), "NVARCHAR(50)");
        Assert.assertEquals(attribute.getDisplayName(), "Add This");
        Assert.assertEquals(attribute.getDataSource(), "BuiltWith_Pivoted_Source");
        Assert.assertEquals(attribute.getDescription(), "Tech Indicator Add This");
        attribute = attributes.get(1);
        Assert.assertEquals(attribute.getFieldName(), "TechIndicator_RemoveThis");
        Assert.assertEquals(attribute.getFieldType(), "NVARCHAR(100)");
        Assert.assertEquals(attribute.getDisplayName(), "Remove This");
        Assert.assertEquals(attribute.getDataSource(), "HGData");
        Assert.assertEquals(attribute.getDescription(), "Tech Indicator Remove This");
        Assert.assertEquals(attributes.get(1).getFieldName(), "TechIndicator_RemoveThis");
    }
}
