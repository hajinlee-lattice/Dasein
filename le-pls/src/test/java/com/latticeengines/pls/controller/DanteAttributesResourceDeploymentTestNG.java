package com.latticeengines.pls.controller;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooDefs;
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
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

import edu.emory.mathcs.backport.java.util.Arrays;

public class DanteAttributesResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Value("${camille.zk.connectionString}")
    private String zkConnectionString;

    private Path metadataDocumentPath;

    @BeforeClass
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
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
                .getSystemResource("com/latticeengines/pls/functionalframework/MetadataDocument.json").getFile()),
                Charset.defaultCharset());

        Document doc = new Document();
        doc.setData(metadataDoc);
        camille.create(metadataDocumentPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testGetAccountAttributes() {
        List<DanteAttribute> rawAttributes = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/accountattributes", //
                List.class);
        List<DanteAttribute> attributes = JsonUtils.convertList(rawAttributes, DanteAttribute.class);

        Assert.assertNotNull(attributes);
        Assert.assertEquals(27, attributes.size());
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testGetRecommendationAttributes() {
        List<DanteAttribute> rawAttributes = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                List.class);
        List<DanteAttribute> attributes = JsonUtils.convertList(rawAttributes, DanteAttribute.class);

        Assert.assertNotNull(attributes);
        Assert.assertEquals(8, attributes.size());
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void testAttributes() {
        List<String> notions = Arrays
                .asList(new String[] { "RecoMMendation", "accOUNT", "something", "invalid", "account", "account" });
        DanteNotionAttributes notionAttributes = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes", //
                notions, DanteNotionAttributes.class);
        Assert.assertNotNull(notionAttributes);
        Assert.assertNotNull(notionAttributes.getInvalidNotions());
        Assert.assertEquals(notionAttributes.getInvalidNotions().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("account").size(), 27);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
    }

    @AfterClass
    public void teardown() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(metadataDocumentPath);
    }
}
