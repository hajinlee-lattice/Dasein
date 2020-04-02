package com.latticeengines.apps.dcp.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

public class UploadServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private UploadService uploadService;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreateGetUpdate() {
        String sourceId1 = "sourceId1";
        String sourceId2 = "sourceId2";
        Upload upload1 = uploadService.createUpload(mainCustomerSpace, sourceId1, null);
        Upload upload2 = uploadService.createUpload(mainCustomerSpace, sourceId1, null);
        Upload upload3 = uploadService.createUpload(mainCustomerSpace, sourceId2, null);
        Assert.assertNotNull(upload1.getPid());
        Assert.assertNotNull(upload2.getPid());
        Assert.assertNotNull(upload3.getPid());
        List<Upload> uploads = uploadService.getUploads(mainCustomerSpace, sourceId1);
        Assert.assertTrue(CollectionUtils.isNotEmpty(uploads));
        Assert.assertEquals(uploads.size(), 2);
        uploads.forEach(upload -> {
            Assert.assertNull(upload.getUploadConfig());
            Assert.assertEquals(upload.getStatus(), Upload.Status.NEW);
        });
        UploadConfig uploadConfig = new UploadConfig();
        uploadConfig.setDropFilePath("DummyPath");
        uploadService.updateUploadConfig(mainCustomerSpace, upload1.getPid(), uploadConfig);
        uploadService.updateUploadStatus(mainCustomerSpace, upload2.getPid(), Upload.Status.IMPORT_STARTED);
        uploads = uploadService.getUploads(mainCustomerSpace, sourceId1);
        Assert.assertEquals(uploads.size(), 2);
        uploads.forEach(upload -> {
            if (upload.getPid().longValue() == upload1.getPid().longValue()) {
                Assert.assertNotNull(upload.getUploadConfig());
            } else {
                Assert.assertEquals(upload.getStatus(), Upload.Status.IMPORT_STARTED);
            }
        });

        // update match result table
        Table table = createTable();
        uploadService.registerMatchResult(mainCustomerSpace, upload1.getPid(), table.getName());
        Upload uploadWithTable = uploadService.getUpload(mainCustomerSpace, upload1.getPid());
        String matchedTableName = uploadWithTable.getMatchResultName();
        Assert.assertEquals(matchedTableName, table.getName());
        Table matchedTable = tableEntityMgr.findByName(matchedTableName);
        Assert.assertNotNull(matchedTable);
        Assert.assertEquals(matchedTable.getTenant().getId(), mainTestTenant.getId());
        Assert.assertEquals(matchedTable.getAttributes().size(), 1);
        Table table2 = createTable();
        uploadService.registerMatchResult(mainCustomerSpace, upload1.getPid(), table2.getName());
        uploadWithTable = uploadService.getUpload(mainCustomerSpace, upload1.getPid());
        matchedTableName = uploadWithTable.getMatchResultName();
        Assert.assertEquals(matchedTableName, table2.getName());
        table = tableEntityMgr.findByName(table.getName());
        Assert.assertNotNull(table);
        Assert.assertNotNull(table.getRetentionPolicy());

        Upload uploadx = uploadService.getUpload(mainCustomerSpace, upload1.getPid());
        Assert.assertNotNull(uploadx);
        Assert.assertEquals(uploadx.getPid(), upload1.getPid());
    }

    protected Table createTable() {
        String tableName = NamingUtils.timestampWithRandom("TestTable");

        Table newTable = new Table();
        newTable.setName(tableName);
        newTable.setDisplayName(tableName);
        newTable.setTenant(mainTestTenant);
        newTable.setTableType(TableType.DATATABLE);

        Attribute attribute = new Attribute();
        attribute.setName("Attr1");
        attribute.setDisplayName("My Attr1");
        attribute.setPhysicalDataType("long");
        newTable.setAttributes(Collections.singletonList(attribute));

        tableEntityMgr.create(newTable);

        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        return retry.execute(ctx -> {
            Table tbl = tableEntityMgr.findByName(tableName);
            Assert.assertNotNull(tbl);
            return tbl;
        });
    }
}
