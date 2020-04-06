package com.latticeengines.apps.dcp.service.impl;

import java.util.Arrays;
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
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;
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
        List<Upload> uploads = createUploads();
        Upload upload1 = uploads.get(0);
        Upload uploadx = uploadService.getUpload(mainCustomerSpace, upload1.getPid());
        Assert.assertNotNull(uploadx);
        Assert.assertEquals(uploadx.getPid(), upload1.getPid());

        updateUploadStats(upload1);
        updateMatchResultTable(upload1);
    }

    private List<Upload> createUploads() {
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
        return Arrays.asList(upload1, upload2);
    }

    private void updateMatchResultTable(Upload upload) {
        Table table = createTable();
        uploadService.registerMatchResult(mainCustomerSpace, upload.getPid(), table.getName());
        Upload uploadWithTable = uploadService.getUpload(mainCustomerSpace, upload.getPid());
        String matchedTableName = uploadWithTable.getMatchResultName();
        Assert.assertEquals(matchedTableName, table.getName());
        Table matchedTable = tableEntityMgr.findByName(matchedTableName);
        Assert.assertNotNull(matchedTable);
        Assert.assertEquals(matchedTable.getTenant().getId(), mainTestTenant.getId());
        Assert.assertEquals(matchedTable.getAttributes().size(), 1);
        Table table2 = createTable();
        uploadService.registerMatchResult(mainCustomerSpace, upload.getPid(), table2.getName());
        uploadWithTable = uploadService.getUpload(mainCustomerSpace, upload.getPid());
        matchedTableName = uploadWithTable.getMatchResultName();
        Assert.assertEquals(matchedTableName, table2.getName());
        table = tableEntityMgr.findByName(table.getName());
        Assert.assertNotNull(table);
        Assert.assertNotNull(table.getRetentionPolicy());
    }

    private void updateUploadStats(Upload upload) {
        Long workflowId = createFakeWorkflow(mainTestTenant.getPid());

        UploadStatsContainer container0 = new UploadStatsContainer();
        container0.setWorkflowPid(workflowId);
        UploadStats stats = new UploadStats();
        UploadStats.ImportStats importStats = new UploadStats.ImportStats();
        importStats.setErrorCnt(1L);
        stats.setImportStats(importStats);
        container0.setStatistics(stats);

        UploadStatsContainer container = uploadService.appendStatistics(upload.getPid(), container0);
        Assert.assertNotNull(container);
        Assert.assertNotNull(container.getPid());
        Assert.assertEquals(container.getWorkflowPid(), workflowId);

        Upload upload2 = uploadService.getUpload(mainCustomerSpace, upload.getPid());
        Assert.assertNull(upload2.getStatistics());

        upload2 = uploadService.setLatestStatistics(upload.getPid(), container.getPid());
        Assert.assertNotNull(upload2.getStatistics());
        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        Upload upload3 = retry.execute(ctx -> {
            Upload u = uploadService.getUpload(mainCustomerSpace, upload.getPid());
            Assert.assertNotNull(u.getStatistics());
            return u;
        });

        Assert.assertNotNull(upload3.getStatistics());
        Assert.assertEquals(upload3.getStatistics().getImportStats().getErrorCnt(), Long.valueOf(1));

        UploadStatsContainer container1 = new UploadStatsContainer();
        container1.setWorkflowPid(workflowId);
        stats = new UploadStats();
        importStats = new UploadStats.ImportStats();
        importStats.setErrorCnt(3L);
        stats.setImportStats(importStats);
        container1.setStatistics(stats);
        uploadService.appendStatistics(upload.getPid(), container1);
        Assert.assertNotEquals(container1.getPid(), container.getPid());
        Upload upload4 = uploadService.setLatestStatistics(upload.getPid(), container1.getPid());
        Assert.assertEquals(upload4.getStatistics().getImportStats().getErrorCnt(), Long.valueOf(3));
        retry.execute(ctx -> {
            Upload u = uploadService.getUpload(mainCustomerSpace, upload.getPid());
            Assert.assertNotNull(u.getStatistics());
            Assert.assertEquals(u.getStatistics().getImportStats().getErrorCnt(), Long.valueOf(3));
            return u;
        });
    }

    private Table createTable() {
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
