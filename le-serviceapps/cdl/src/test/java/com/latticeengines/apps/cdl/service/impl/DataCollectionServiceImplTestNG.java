package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;

public class DataCollectionServiceImplTestNG extends CDLFunctionalTestNGBase {
    @Inject
    private DataCollectionService dataCollectionService;

    private DataCollection.Version version = DataCollection.Version.Green;
    private String name1 = "artiface1";
    private String url1 = "https://s3.amazon.com/artifact1/Green";
    private String name2 = "artiface2";
    private String url2 = "https://s3.amazon.com/artifact2/Green";

    private DataCollectionArtifact artifact1;
    private DataCollectionArtifact artifact2;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        dataCollectionService.deleteArtifact(mainCustomerSpace, name1, version);
        dataCollectionService.deleteArtifact(mainCustomerSpace, name2, version);
        dataCollectionEntityMgr.delete(dataCollection);
    }

    @Test(groups = "functional", priority = 0)
    public void testCreateArtifact() {
        artifact1 = dataCollectionService.createArtifact(mainCustomerSpace, name1, url1, version);
        artifact2 = dataCollectionService.createArtifact(mainCustomerSpace, name2, url2, version);
        Assert.assertNotNull(artifact1);
        Assert.assertEquals(artifact1.getName(), name1);
        Assert.assertEquals(artifact1.getVersion(), version);
        Assert.assertNotNull(artifact2);
        Assert.assertEquals(artifact2.getUrl(), url2);
    }

    @Test(groups = "functional", priority = 1)
    public void testFindOneArtifact() {
        DataCollectionArtifact artifact = dataCollectionService.getArtifact(mainCustomerSpace, name1, version);
        Assert.assertNotNull(artifact);
        Assert.assertEquals(artifact.getName(), name1);
        Assert.assertEquals(artifact.getUrl(), url1);
        Assert.assertEquals(artifact.getVersion(), version);
    }

    @Test(groups = "functional", priority = 2)
    public void testFindManyArtifacts() {
        List<DataCollectionArtifact> artifacts = dataCollectionService.getArtifacts(mainCustomerSpace, version);
        Assert.assertNotNull(artifacts);
        Assert.assertEquals(artifacts.size(), 2);
        Assert.assertEquals(artifacts.get(0).getName(), name1);
        Assert.assertEquals(artifacts.get(1).getUrl(), url2);
    }

    @Test(groups = "functional", priority = 3)
    public void testDeleteArtifact() {
        DataCollectionArtifact artifact = dataCollectionService.createArtifact(mainCustomerSpace,
                "test", "https://url.com", version);
        Assert.assertNotNull(artifact);
        artifact = dataCollectionService.deleteArtifact(mainCustomerSpace, artifact.getName(), artifact.getVersion());
        Assert.assertNotNull(artifact);
    }
}
