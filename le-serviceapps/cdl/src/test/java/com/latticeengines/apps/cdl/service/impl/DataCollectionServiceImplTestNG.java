package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class DataCollectionServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataCollectionService dataCollectionService;

    private DataCollection.Version version = DataCollection.Version.Green;
    private String name1 = "artifact1";
    private String url1 = "https://s3.amazon.com/artifact1/Green";
    private String name2 = "artifact2";
    private String url2 = "https://s3.amazon.com/artifact2/Green";

    private DataCollectionArtifact artifact1;
    private DataCollectionArtifact artifact2;

    private DataCollectionTable dataCollectionTable;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        Table table = getTable();
        tableEntityMgr.create(table);
        dataCollectionTable = dataCollectionEntityMgr.upsertTableToCollection(collectionName, table.getName(),
                TableRoleInCollection.ConsolidatedAccount, DataCollection.Version.Blue);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        dataCollectionService.deleteArtifact(mainCustomerSpace, name1, version, true);
        dataCollectionService.deleteArtifact(mainCustomerSpace, name2, version, true);
        dataCollectionEntityMgr.delete(dataCollection);
    }

    @Test(groups = "functional", priority = 0)
    public void testCreateArtifact() {
        artifact1 = dataCollectionService.createArtifact(mainCustomerSpace, name1, url1,
                DataCollectionArtifact.Status.NOT_SET, version);
        artifact2 = dataCollectionService.createArtifact(mainCustomerSpace, name2, url2,
                DataCollectionArtifact.Status.NOT_SET, version);
        Assert.assertNotNull(artifact1);
        Assert.assertEquals(artifact1.getName(), name1);
        Assert.assertEquals(artifact1.getVersion(), version);
        Assert.assertEquals(artifact1.getStatus(), DataCollectionArtifact.Status.NOT_SET);

        Assert.assertNotNull(artifact2);
        Assert.assertEquals(artifact2.getName(), name2);
        Assert.assertEquals(artifact2.getUrl(), url2);
        Assert.assertEquals(artifact2.getStatus(), DataCollectionArtifact.Status.NOT_SET);
    }

    @Test(groups = "functional", priority = 1)
    public void testFindOneArtifact() {
        DataCollectionArtifact artifact = dataCollectionService.getLatestArtifact(mainCustomerSpace, name1, version);
        Assert.assertNotNull(artifact);
        Assert.assertEquals(artifact.getName(), name1);
        Assert.assertEquals(artifact.getUrl(), url1);
        Assert.assertEquals(artifact.getVersion(), version);
        Assert.assertEquals(artifact.getStatus(), DataCollectionArtifact.Status.NOT_SET);
    }

    @Test(groups = "functional", priority = 2)
    public void testFindManyArtifactsNoStatus() {
        List<DataCollectionArtifact> artifacts = dataCollectionService.getArtifacts(mainCustomerSpace, null, version);
        Assert.assertNotNull(artifacts);
        Assert.assertEquals(artifacts.size(), 2);
    }

    @Test(groups = "functional", priority = 3)
    public void testUpdateArtifact() {
        artifact1.setStatus(DataCollectionArtifact.Status.GENERATING);
        DataCollectionArtifact artifact = dataCollectionService.updateArtifact(mainCustomerSpace, artifact1);
        Assert.assertEquals(artifact.getName(), artifact1.getName());
        Assert.assertEquals(artifact.getStatus(), artifact1.getStatus());
        Assert.assertEquals(artifact.getUrl(), artifact1.getUrl());

        artifact = dataCollectionService.getLatestArtifact(mainCustomerSpace, name1, version);
        Assert.assertEquals(artifact.getStatus(), DataCollectionArtifact.Status.GENERATING);
    }

    @Test(groups = "functional", priority = 4)
    public void testFindManyArtifactsWithStatus() {
        DataCollectionArtifact.Status status = DataCollectionArtifact.Status.NOT_SET;
        List<DataCollectionArtifact> artifacts = dataCollectionService.getArtifacts(mainCustomerSpace, status, version);
        Assert.assertNotNull(artifacts);
        Assert.assertEquals(artifacts.size(), 1);
        Assert.assertEquals(artifacts.get(0).getName(), name2);
        Assert.assertEquals(artifacts.get(0).getUrl(), url2);
        Assert.assertEquals(artifacts.get(0).getStatus(), DataCollectionArtifact.Status.NOT_SET);

        status = DataCollectionArtifact.Status.GENERATING;
        artifacts = dataCollectionService.getArtifacts(mainCustomerSpace, status, version);
        Assert.assertNotNull(artifacts);
        Assert.assertEquals(artifacts.size(), 1);
        Assert.assertEquals(artifacts.get(0).getName(), name1);
        Assert.assertEquals(artifacts.get(0).getUrl(), url1);
        Assert.assertEquals(artifacts.get(0).getStatus(), DataCollectionArtifact.Status.GENERATING);
    }

    @Test(groups = "functional", priority = 5)
    public void testDeleteArtifact() {
        DataCollectionArtifact artifact = dataCollectionService.createArtifact(mainCustomerSpace,
                "test", "https://url.com", DataCollectionArtifact.Status.NOT_SET, version);
        Assert.assertNotNull(artifact);
        artifact = dataCollectionService.deleteArtifact(mainCustomerSpace, artifact.getName(), artifact.getVersion(), true);
        Assert.assertNotNull(artifact);
    }

    @Test(groups = "functional")
    public void testDiagnostic() {
        ImportTemplateDiagnostic diagnostic = dataCollectionService.diagnostic(mainCustomerSpace, dataCollectionTable.getPid());
        Assert.assertNotNull(diagnostic);
        Assert.assertEquals(diagnostic.getWarnings().size(), 2);
        Assert.assertTrue(diagnostic.getWarnings().get(0).contains("user_Att2")
                || diagnostic.getWarnings().get(1).contains("user_Att2"));
        Assert.assertTrue(diagnostic.getWarnings().get(0).contains("NumberOfEmployees")
                || diagnostic.getWarnings().get(1).contains("NumberOfEmployees"));
    }

    private Table getTable() {
        Table table = SchemaRepository.instance().getSchema(BusinessEntity.Account, true, false, true);
        table.setTableType(TableType.DATATABLE);
        Attribute userAttr1 = new Attribute();
        userAttr1.setPhysicalDataType("String");
        userAttr1.setFundamentalType(FundamentalType.ALPHA);
        userAttr1.setName("user_Att1");
        userAttr1.setDisplayName("Attr1");
        table.addAttribute(userAttr1);
        Attribute userAttr2 = new Attribute();
        userAttr2.setPhysicalDataType("Int");
        userAttr2.setFundamentalType(FundamentalType.ALPHA);
        userAttr2.setName("user_Att2");
        userAttr2.setDisplayName("Attr2");
        table.addAttribute(userAttr2);
        for (Attribute attr : table.getAttributes()) {
            if (attr.getName().equalsIgnoreCase("NumberOfEmployees")) {
                attr.setFundamentalType(FundamentalType.ALPHA);
            }
        }
        return table;
    }
}
