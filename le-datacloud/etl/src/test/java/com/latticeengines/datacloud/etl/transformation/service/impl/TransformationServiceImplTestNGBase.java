package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class TransformationServiceImplTestNGBase<T extends TransformationConfiguration> extends
        DataCloudEtlFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TransformationServiceImplTestNGBase.class);

    private static final int MAX_LOOPS = 100;

    @Inject
    TransformationProgressEntityMgr progressEntityMgr;

    @Inject
    protected PipelineTransformationService pipelineTransformationService;

    @Inject
    protected SourceService sourceService;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    Source source;

    TransformationService<T> transformationService;

    Collection<TransformationProgress> progresses = new HashSet<>();
    private Date yesterday = new Date(new Date().getTime() - TimeUnit.DAYS.toMillis(1));
    protected String baseSourceVersion = HdfsPathBuilder.dateFormat.format(yesterday);
    protected String targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
    protected Calendar calendar = GregorianCalendar.getInstance();

    protected abstract TransformationService<T> getTransformationService();

    protected abstract Source getSource();

    protected abstract String getPathToUploadBaseData();

    protected abstract T createTransformationConfiguration();

    protected abstract String getPathForResult();

    protected abstract void verifyResultAvroRecords(Iterator<GenericRecord> records);

    @BeforeMethod(groups = { "functional", "deployment", "pipeline1", "pipeline2" })
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source.getSourceName());
        transformationService = getTransformationService();
    }

    protected void uploadFileToHdfs(List<String> fileNames) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, getPathToUploadBaseData())) {
                HdfsUtils.rmdir(yarnConfiguration, getPathToUploadBaseData());
            }
            for (String fileName : fileNames) {
                InputStream fileStream = ClassLoader.getSystemResourceAsStream("sources/" + fileName);
                String targetPath = getPathToUploadBaseData() + "/" + fileName;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
                InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
                String successPath = getPathToUploadBaseData() + SUCCESS_FLAG;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void uploadAndRegisterTableSource(String avroFile, String tableName) {
        uploadAndRegisterTableSource(Collections.singletonList(avroFile), tableName);
    }

    protected void uploadAndRegisterTableSource(String avroFile, String tableName, String primaryKeyName,
            String lastModifiedKeyName) {
        uploadAndRegisterTableSource(Collections.singletonList(avroFile), tableName, primaryKeyName,
                lastModifiedKeyName);
    }

    protected void uploadAndRegisterTableSource(List<String> avroFiles, String tableName) {
        uploadAndRegisterTableSource(avroFiles, tableName, null, null);
    }

    protected void uploadAndRegisterTableSource(List<String> avroFiles, String tableName, String primaryKeyName,
            String lastModifiedKeyName) {
        String tableDir = hdfsPathBuilder.constructTablePath(tableName,
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE), "").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, tableDir)) {
                HdfsUtils.rmdir(yarnConfiguration, tableDir);
            }
            for (String fileName : avroFiles) {
                if (!fileName.endsWith(".avro")) {
                    fileName = fileName + ".avro";
                }
                InputStream fileStream = ClassLoader.getSystemResourceAsStream("sources/" + fileName);
                String targetPath = tableDir + "/" + fileName;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
            }
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String uploadBaseDir = getPathToUploadBaseData();
            String successPath = null;
            if (uploadBaseDir != null) {
                successPath = getPathToUploadBaseData() + SUCCESS_FLAG;
            } else {
                successPath = tableDir + SUCCESS_FLAG;
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Table table = MetadataConverter.getTable(yarnConfiguration, tableDir, primaryKeyName, lastModifiedKeyName);
        table.setName(tableName);
        metadataProxy.updateTable(DataCloudConstants.SERVICE_CUSTOMERSPACE, tableName, table);
    }

    protected void uploadAndRegisterTableSource(List<Pair<String, Class<?>>> schema, Object[][] data,
            String tableName) {
        uploadAndRegisterTableSource(schema, data, tableName, null, null);
    }

    protected void uploadAndRegisterTableSource(List<Pair<String, Class<?>>> schema, Object[][] data, String tableName,
            String primaryKeyName, String lastModifiedKeyName) {
        String tableDir = hdfsPathBuilder
                .constructTablePath(tableName, CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE), "")
                .toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, tableDir)) {
                HdfsUtils.rmdir(yarnConfiguration, tableDir);
            }
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, tableDir, "part-00000.avro");
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String uploadBaseDir = getPathToUploadBaseData();
            String successPath = null;
            if (uploadBaseDir != null) {
                successPath = getPathToUploadBaseData() + SUCCESS_FLAG;
            } else {
                successPath = tableDir + SUCCESS_FLAG;
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);

            Schema avroSchema = AvroUtils.constructSchema(tableName, schema);
            String avscPath = hdfsPathBuilder.constructTableSchemaFilePath(tableName,
                    CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE), "").toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, avroSchema.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Table table = MetadataConverter.getTable(yarnConfiguration, tableDir, primaryKeyName, lastModifiedKeyName);
        table.setName(tableName);
        metadataProxy.updateTable(DataCloudConstants.SERVICE_CUSTOMERSPACE, tableName, table);
    }

    protected List<GenericRecord> getRecordFromTable(String tableName) {
        String tableDir = hdfsPathBuilder.constructTablePath(tableName,
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE), "").toString();
        return AvroUtils.getDataFromGlob(yarnConfiguration, tableDir + "/*.avro");
    }

    protected void cleanRecordFromTable(String tableName) {
        String tableDir = hdfsPathBuilder.constructTablePath(tableName,
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE), "").toString();
        try {
            HdfsUtils.rmdir(yarnConfiguration, tableDir);
        } catch (Exception ex) {
            log.warn("can not delete table dir=" + tableDir, ex);
        }
    }

    protected TransformationProgress createNewProgress() {
        TransformationProgress progress = transformationService.startNewProgress(createTransformationConfiguration(),
                progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected TransformationProgress transformData(TransformationProgress progress) {
        TransformationProgress response = transformationService
                .transform(progress, createTransformationConfiguration());

        Assert.assertEquals(response.getStatus(), ProgressStatus.FINISHED);

        TransformationProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress
                .getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return response;
    }

    protected TransformationProgress finish(TransformationProgress progress) {
        TransformationProgress progressInDb = null;
        for (int i = 0; i < MAX_LOOPS; i++) {
            progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
            Assert.assertNotNull(progressInDb);
            if (progressInDb.getStatus().equals(ProgressStatus.FINISHED)) {
                break;
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return progressInDb;
    }

    protected void confirmResultFile(TransformationProgress progress) {
        String path = getPathForResult();
        log.info("Checking for result file: " + path);
        Iterator<GenericRecord> records = getGenericRecords(path);
        verifyResultAvroRecords(records);
    }

    protected void confirmIntermediateSource(Source source, String version) {
        if (StringUtils.isBlank(version)) {
            version = hdfsSourceEntityMgr.getCurrentVersion(source);
        }
        String path = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
        log.info(String.format("Checking result of intermediate source %s @%s: %s", source.getSourceName(), version,
                path));
        Iterator<GenericRecord> records = getGenericRecords(path);
        verifyIntermediateResult(source.getSourceName(), version, records);
    }

    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {

    }

    protected Iterator<GenericRecord> iterateSource(String sourceName) {
        String path = getPathForSource(sourceName);
        System.out.println("Checking for result file: " + path);
        List<String> files;
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(files.size() >= 2);
        for (String file : files) {
            if (!file.endsWith(SUCCESS_FLAG)) {
                Assert.assertTrue(file.endsWith(".avro"));
                continue;
            }
            Assert.assertTrue(file.endsWith(SUCCESS_FLAG));
        }

        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        return records;
    }

    protected String getPathForSource(String sourceName) {
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(sourceName);
        return hdfsPathBuilder.constructSnapshotDir(sourceName, targetVersion).toString();
    }

    protected void cleanupProgressTables() {
        for (TransformationProgress progress : progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

}
