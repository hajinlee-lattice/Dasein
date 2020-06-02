package com.latticeengines.apps.dcp.service.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class UploadServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private UploadService uploadService;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        WorkflowProxy workflowProxy = mock(WorkflowProxy.class);
        Job job1 = new Job();
        job1.setApplicationId("application_1553048841184_0001");
        when(workflowProxy.getJobByWorkflowJobPid(anyString(), anyLong())).thenReturn(job1);
        ReflectionTestUtils.setField(uploadService, "workflowProxy", workflowProxy);
    }

    @Test(groups = "functional")
    public void testCreateGetUpdate() {
        List<UploadDetails> uploads = createUploads();
        UploadDetails upload1 = uploads.get(0);
        UploadDetails uploadx = uploadService.getUploadByUploadId(mainCustomerSpace, upload1.getUploadId());
        Assert.assertNotNull(uploadx);
        Assert.assertEquals(uploadx.getUploadId(), upload1.getUploadId());

        updateUploadStats(upload1);
        updateMatchResultTable(upload1);
    }

    private List<UploadDetails> createUploads() {
        String sourceId1 = "sourceId1";
        String sourceId2 = "sourceId2";
        UploadDetails upload1 = uploadService.createUpload(mainCustomerSpace, sourceId1, null);
        UploadDetails upload2 = uploadService.createUpload(mainCustomerSpace, sourceId1, null);
        UploadDetails upload3 = uploadService.createUpload(mainCustomerSpace, sourceId2, null);
        Assert.assertNotNull(upload1.getUploadId());
        Assert.assertNotNull(upload2.getUploadId());
        Assert.assertNotNull(upload3.getUploadId());
        List<UploadDetails> uploads = uploadService.getUploads(mainCustomerSpace, sourceId1);
        Assert.assertTrue(CollectionUtils.isNotEmpty(uploads));
        Assert.assertEquals(uploads.size(), 2);
        uploads.forEach(upload -> {
            Assert.assertNull(upload.getUploadConfig());
            Assert.assertEquals(upload.getUploadStatus().getStatus(), Upload.Status.NEW);
        });
        UploadConfig uploadConfig = new UploadConfig();
        uploadConfig.setDropFilePath("DummyPath");
        uploadService.updateUploadConfig(mainCustomerSpace, upload1.getUploadId(), uploadConfig);
        uploadService.updateUploadStatus(mainCustomerSpace, upload2.getUploadId(), Upload.Status.IMPORT_STARTED);
        uploads = uploadService.getUploads(mainCustomerSpace, sourceId1);
        Assert.assertEquals(uploads.size(), 2);
        uploads.forEach(upload -> {
            if (upload.getUploadId().equals(upload1.getUploadId())) {
                Assert.assertNotNull(upload.getUploadConfig());
            } else {
                Assert.assertEquals(upload.getUploadStatus().getStatus(), Upload.Status.IMPORT_STARTED);
            }
        });
        return Arrays.asList(upload1, upload2);
    }

    private void updateMatchResultTable(UploadDetails upload) {
        String uploadId = upload.getUploadId();
        String matchResult = uploadService.getMatchResultTableName(uploadId);
        Assert.assertTrue(StringUtils.isBlank(matchResult));

        Table table = createTable();
        uploadService.registerMatchResult(mainCustomerSpace, uploadId, table.getName());
        matchResult = uploadService.getMatchResultTableName(uploadId);
        Assert.assertEquals(matchResult, table.getName());
        Table matchedTable = tableEntityMgr.findByName(matchResult);
        Assert.assertNotNull(matchedTable);
        Assert.assertEquals(matchedTable.getTenant().getId(), mainTestTenant.getId());

        Table table2 = createTable();
        uploadService.registerMatchResult(mainCustomerSpace, uploadId, table2.getName());
        matchResult = uploadService.getMatchResultTableName(uploadId);
        Assert.assertEquals(matchResult, table2.getName());
        matchedTable = tableEntityMgr.findByName(matchResult);
        Assert.assertNotNull(matchedTable);

        Table oldTable = tableEntityMgr.findByName(table.getName());
        Assert.assertNotNull(oldTable.getRetentionPolicy());
    }

    private void updateUploadStats(UploadDetails upload) {
        Long workflowId = createFakeWorkflow(mainTestTenant.getPid());

        UploadStatsContainer container0 = new UploadStatsContainer();
        container0.setWorkflowPid(workflowId);
        UploadStats stats = new UploadStats();
        UploadStats.ImportStats importStats = new UploadStats.ImportStats();
        importStats.setErrorCnt(1L);
        stats.setImportStats(importStats);
        container0.setStatistics(stats);

        UploadStatsContainer container = uploadService.appendStatistics(upload.getUploadId(), container0);
        Assert.assertNotNull(container);
        Assert.assertNotNull(container.getPid());
        Assert.assertEquals(container.getWorkflowPid(), workflowId);

        UploadDetails upload2 = uploadService.getUploadByUploadId(mainCustomerSpace, upload.getUploadId());
        Assert.assertNull(upload2.getStatistics());

        upload2 = uploadService.setLatestStatistics(mainCustomerSpace, upload.getUploadId(), container.getPid());
        Assert.assertNotNull(upload2.getStatistics());
        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        UploadDetails upload3 = retry.execute(ctx -> {
            UploadDetails u = uploadService.getUploadByUploadId(mainCustomerSpace, upload.getUploadId());
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
        uploadService.appendStatistics(upload.getUploadId(), container1);
        Assert.assertNotEquals(container1.getPid(), container.getPid());
        UploadDetails upload4 = uploadService.setLatestStatistics(mainCustomerSpace, upload.getUploadId(), container1.getPid());
        Assert.assertEquals(upload4.getStatistics().getImportStats().getErrorCnt(), Long.valueOf(3));
        retry.execute(ctx -> {
            UploadDetails u = uploadService.getUploadByUploadId(mainCustomerSpace, upload.getUploadId());
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
