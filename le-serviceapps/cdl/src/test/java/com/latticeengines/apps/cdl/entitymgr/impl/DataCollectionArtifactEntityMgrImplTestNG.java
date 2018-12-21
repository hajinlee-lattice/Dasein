package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionArtifactEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;

public class DataCollectionArtifactEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataCollectionArtifactEntityMgr dataCollectionArtifactEntityMgr;

    private DataCollectionArtifact artifact1;
    private DataCollectionArtifact artifact2;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        dataCollectionArtifactEntityMgr.delete(artifact1);
        dataCollectionArtifactEntityMgr.delete(artifact2);
        dataCollectionEntityMgr.delete(dataCollection);
    }

    @Test(groups = "functional")
    public void testCRUD() throws Exception {
        String artifactName1 = "artifact 1";
        String artifactName2 = "artifact 2";
        DataCollection.Version testVersion = DataCollection.Version.Blue;

        artifact1 = new DataCollectionArtifact();
        artifact1.setCreateTime(System.currentTimeMillis());
        artifact1.setDataCollection(dataCollection);
        artifact1.setTenant(mainTestTenant);
        artifact1.setName(artifactName1);
        artifact1.setUrl("https://s3.amazon.com/artifact1/Blue");
        artifact1.setVersion(testVersion);
        artifact1.setStatus(DataCollectionArtifact.Status.GENERATING);
        dataCollectionArtifactEntityMgr.create(artifact1);
        Thread.sleep(500L);

        artifact2 = new DataCollectionArtifact();
        artifact2.setCreateTime(System.currentTimeMillis());
        artifact2.setDataCollection(dataCollection);
        artifact2.setTenant(mainTestTenant);
        artifact2.setName(artifactName2);
        artifact2.setUrl("https://s3.amazon.com/artifact2/Blue");
        artifact2.setVersion(testVersion);
        artifact2.setStatus(DataCollectionArtifact.Status.NOT_SET);
        dataCollectionArtifactEntityMgr.create(artifact2);
        Thread.sleep(500L);

        List<DataCollectionArtifact> artifacts = dataCollectionArtifactEntityMgr.findByTenantAndVersion(
                mainTestTenant, testVersion);
        Assert.assertNotNull(artifacts);
        Assert.assertEquals(artifacts.size(), 2);
        Assert.assertTrue(artifacts.get(0).getCreateTime() > artifacts.get(1).getCreateTime());
        Assert.assertEquals(artifacts.get(0).getName(), artifactName2);
        Assert.assertEquals(artifacts.get(1).getName(), artifactName1);

        String newArtifactName = "artifact 1000";
        String newArtifactUrl = "https://s3.amazon.com/artifact1000/Blue";
        artifact1.setName(newArtifactName);
        artifact1.setUrl(newArtifactUrl);
        artifact1.setStatus(DataCollectionArtifact.Status.READY);
        dataCollectionArtifactEntityMgr.createOrUpdate(artifact1);
        Thread.sleep(500L);

        List<DataCollectionArtifact> retrieved = dataCollectionArtifactEntityMgr.findByTenantAndNameAndVersion(
                mainTestTenant, newArtifactName, testVersion);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.size(), 1);
        Assert.assertEquals(retrieved.get(0).getName(), newArtifactName);
        Assert.assertEquals(retrieved.get(0).getUrl(), newArtifactUrl);
    }
}
