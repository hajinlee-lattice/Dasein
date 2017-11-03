package com.latticeengines.dante.controller;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.testFramework.DanteTestNGBase;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;
import com.latticeengines.proxy.exposed.dante.DanteAttributesProxy;

public class DanteAttributesResourceDeploymentTestNG extends DanteTestNGBase {

    @Autowired
    private DanteAttributesProxy danteAttributesProxy;

    @Value("${camille.zk.connectionString}")
    private String zkConnectionString;

    private Path metadataDocumentPath;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        CamilleConfiguration config = new CamilleConfiguration();
        config.setConnectionString(zkConnectionString);
        CamilleEnvironment.start(CamilleEnvironment.Mode.RUNTIME, config);
        Camille camille = CamilleEnvironment.getCamille();
        metadataDocumentPath = PathBuilder
                .buildCustomerSpacePath(CamilleEnvironment.getPodId(), mainTestTenant.getName(),
                        mainTestTenant.getName(), "Production") //
                .append(PathConstants.SERVICES) //
                .append(PathConstants.DANTE) //
                .append(new Path("/MetadataDocument.json"));
        String metadataDoc = FileUtils.readFileToString(new File(ClassLoader
                .getSystemResource("com/latticeengines/dante/testframework/MetadataDocument.json").getFile()),
                Charset.defaultCharset());

        Document doc = new Document();
        doc.setData(metadataDoc);
        camille.create(metadataDocumentPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Test(groups = "deployment")
    public void testGetAccountAttributes() {
        List<DanteAttribute> rawAttributes = danteAttributesProxy
                .getAccountAttributes(mainTestCustomerSpace.toString());
        List<DanteAttribute> attributes = JsonUtils.convertList(rawAttributes, DanteAttribute.class);

        Assert.assertNotNull(attributes);
        Assert.assertEquals(27, attributes.size());
    }

    @Test(groups = "deployment")
    public void testGetRecommendationAttributes() {
        List<DanteAttribute> rawAttributes = danteAttributesProxy
                .getRecommendationAttributes(mainTestCustomerSpace.toString());
        List<DanteAttribute> attributes = JsonUtils.convertList(rawAttributes, DanteAttribute.class);

        Assert.assertNotNull(attributes);
        Assert.assertEquals(8, attributes.size());
    }

    @Test(groups = "deployment")
    public void testAttributes() {
        List<String> notions = Arrays.asList( //
                "RecoMMendation", "accOUNT", "something", "invalid", "account", "account", "Variable" );
        DanteNotionAttributes notionAttributes = danteAttributesProxy.getAttributesByNotions(notions,
                mainTestCustomerSpace.toString());
        Assert.assertNotNull(notionAttributes);
        Assert.assertNotNull(notionAttributes.getInvalidNotions());
        Assert.assertEquals(notionAttributes.getInvalidNotions().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().size(), 3);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("account").size(), 27);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("variable").size(), 5);
    }

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(metadataDocumentPath);
    }
}
