package com.latticeengines.dante.service.impl;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.service.DanteAttributeService;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;

import edu.emory.mathcs.backport.java.util.Arrays;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteAttributeServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private DanteAttributeService danteAttributeService;

    private final Path metadataDocumentPath = new Path("/MetadataDocument.json");

    private final String tenantName = "DanteAttributeTestTenant";

    @BeforeClass
    public void setup() throws Exception {
        CamilleTestEnvironment.start();
        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder
                .buildCustomerSpacePath(CamilleEnvironment.getPodId(), tenantName, tenantName, "Production") //
                .append(PathConstants.SERVICES) //
                .append(PathConstants.DANTE) //
                .append(metadataDocumentPath);
        String metadataDoc = FileUtils.readFileToString(new File(ClassLoader
                .getSystemResource("com/latticeengines/dante/testframework/MetadataDocument.json").getFile()),
                Charset.defaultCharset());

        Document doc = new Document();
        doc.setData(metadataDoc);
        camille.create(docPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Test(groups = "functional")
    public void testGetAccountAttributes() {
        List<DanteAttribute> attributes = danteAttributeService.getAccountAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(27, attributes.size());
    }

    @Test(groups = "functional")
    public void testGetRecommendationAttributes() {
        List<DanteAttribute> attributes = danteAttributeService.getRecommendationAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(8, attributes.size());
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void testAttributes() {
        List<String> notions = Arrays
                .asList(new String[] { "RecoMMendation", "accOUNT", "something", "invalid", "account", "account" });
        DanteNotionAttributes notionAttributes = danteAttributeService.getAttributesForNotions(notions, tenantName);
        String str = JsonUtils.serialize(notionAttributes);
        Assert.assertNotNull(notionAttributes);
        Assert.assertNotNull(notionAttributes.getInvalidNotions());
        Assert.assertEquals(notionAttributes.getInvalidNotions().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("account").size(), 27);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
    }

    @AfterClass
    public void teardown() throws Exception {
        CamilleTestEnvironment.stop();
    }
}
