package com.latticeengines.datacloud.etl.testframework;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-datacloud-etl-context.xml" })
public abstract class DataCloutEtlAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(DataCloutEtlAbstractTestNGBase.class);

    protected static final String SUCCESS_FLAG = "/" + HdfsPathBuilder.SUCCESS_FILE;

    @Value("${datacloud.test.env}")
    protected String testEnv;

    @Value("${datacloud.collection.sqoop.mapper.number:4}")
    private int numMappers;

    @Value("${datacloud.collection.s3bucket}")
    protected String s3Bucket;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private S3Service s3Service;

    @javax.annotation.Resource(name = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

    @javax.annotation.Resource(name = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateBulkDB;

    protected String podId;

    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion) {
        InputStream baseAvroStream = ClassLoader
                .getSystemResourceAsStream("sources/" + baseSource.getSourceName() + ".avro");
        String targetPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, baseSourceVersion)
                .append("part-0000.avro").toString();
        String successPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, baseSourceVersion)
                .append("_SUCCESS").toString();
        uploadToHdfs(targetPath, successPath, baseAvroStream);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceFile(Source baseSource, String baseSourceFile, String baseSourceVersion) {
        InputStream baseSourceStream = null;
        String targetPath = null;
        String successPath = null;
        if (!(baseSource instanceof IngestionSource)) {
            baseSourceFile += ".avro";
        }
        baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSourceFile);
        targetPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, baseSourceVersion)
                .append(baseSourceFile).toString();
        successPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, baseSourceVersion).append("_SUCCESS")
                .toString();
        uploadToHdfs(targetPath, successPath, baseSourceStream);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceFile(String baseSource, String baseSourceFile, String baseSourceVersion) {
        InputStream baseSourceStream = null;
        String targetPath = null;
        String successPath = null;
        if (!baseSourceFile.endsWith(".avro")) {
            baseSourceFile += ".avro";
        }
        baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" +baseSourceFile);
        targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("part-0000.avro")
                .toString();
        successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("_SUCCESS")
                .toString();
        uploadToHdfs(targetPath, successPath, baseSourceStream);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceDir(String baseSource, String baseSourceDir, String baseSourceVersion) throws IOException {
        String rootDir = "sources/" + baseSourceDir;

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] sourceResources = resolver.getResources(rootDir + "/*.avro");
        log.info("Resolved resources for " +rootDir);

        int fileIdx = 0;
        for (Resource resource : sourceResources) {
            if (resource.getURI().toString().endsWith(".avro")) {
                InputStream is = resource.getInputStream();
                String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                        .append(String.format("part-%04d.avro", fileIdx++)).toString();
                log.info("Upload " + resource.getURI().toString() + " to " + targetPath);
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, targetPath);
            }
        }
        String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("_SUCCESS")
                .toString();
        InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    private void uploadToHdfs(String targetPath, String successPath, InputStream baseSourceStream) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseSourceStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void uploadBaseSourceData(Source baseSource, String baseSourceVersion,
            List<Pair<String, Class<?>>> schema,
            Object[][] data) {
        com.latticeengines.domain.exposed.camille.Path targetPath = hdfsPathBuilder
                .constructTransformationSourceDir(baseSource, baseSourceVersion);
        String targetDir = targetPath.toString();
        String successPath = targetPath.append(HdfsPathBuilder.SUCCESS_FILE).toString();
        uploadToHdfs(targetDir, successPath, schema, data);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceData(String baseSource, String baseSourceVersion,
                                        List<Pair<String, Class<?>>> schema, Object[][] data) {
        String targetDir = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                .toString();
        String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                .append("_SUCCESS").toString();
        uploadToHdfs(targetDir, successPath, schema, data);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    private void uploadToHdfs(String targetDir, String successPath, List<Pair<String, Class<?>>> schema,
            Object[][] data) {
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "part-00000.avro");
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void extractSchemaOnHdfs(Source source, String version, String avroDir) throws Exception {
        extractSchemaOnHdfs(source, version, avroDir, null);
    }

    protected void extractSchemaOnHdfs(Source source, String version, String avroDir, Map<String, String> extraProps)
            throws Exception {
        String avscPath;
        if (source instanceof TableSource) {
            TableSource tableSource = (TableSource) source;
            avscPath = hdfsPathBuilder.constructTableSchemaFilePath(tableSource.getTable().getName(),
                    tableSource.getCustomerSpace(), tableSource.getTable().getNamespace()).toString();
        } else {
            avscPath = hdfsPathBuilder.constructSchemaFile(source.getSourceName(), version).toString();
        }
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        Schema parsedSchema = null;
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
        if (files.size() > 0) {
            String avroPath = files.get(0);
            parsedSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
        }

        if (MapUtils.isNotEmpty(extraProps)) {
            for (Map.Entry<String, String> prop : extraProps.entrySet()) {
                parsedSchema.addProp(prop.getKey(), prop.getValue());
            }
        }

        HdfsUtils.writeToFile(yarnConfiguration, avscPath, parsedSchema.toString());
    }

    protected void uploadDataToHdfs(Object[][] data, List<String> colNames, List<Class<?>> colTypes,
            String targetAvroPath, String recordName) {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (int i = 0; i < colNames.size(); i++) {
            schemaMap.put(colNames.get(i), colTypes.get(i));
        }
        Schema schema = AvroUtils.constructSchema(recordName, schemaMap);
        System.out.println(schema.toString());
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            for (int i = 0; i < colNames.size(); i++) {
                builder.set(colNames.get(i), tuple[i]);
            }
            records.add(builder.build());
        }
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetAvroPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetAvroPath);
            }
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetAvroPath, records);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + targetAvroPath, e);
        }
    }

    /**
     * Upload prepared avro files for a source to hdfs
     *
     * @param source:
     *            source object
     * @param version:
     *            version of source which will be set to _CURRENT_VERSION file;
     *            if need to upload multiple versions by calling this method
     *            repeatedly, make sure the latest version is uploaded at last
     * @param fileNames:
     *            data file names (data files should be put under "sources"
     *            folder under "test/resources" directory)
     * @param extractSchema:
     *            whether to generate avsc schema file from avro files uploaded;
     *            require all the uploaded files uploaded are in avro format and
     *            have same schema
     */
    protected void uploadSourceToHdfs(Source source, String version, List<String> fileNames, boolean extractSchema) {
        try {
            String versionPath = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, versionPath)) {
                HdfsUtils.rmdir(yarnConfiguration, versionPath);
            }

            for (String fileName : fileNames) {
                InputStream fileStream = ClassLoader.getSystemResourceAsStream("sources/" + fileName);
                String targetPath = versionPath + "/" + fileName;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
            }

            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = versionPath + SUCCESS_FLAG;
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);

            if (extractSchema) {
                extractSchemaOnHdfs(source, version, versionPath);
            }

            hdfsSourceEntityMgr.setCurrentVersion(source, version);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Upload prepared avro files for a source to s3
     *
     * TODO: To support extracting schema file on s3
     *
     * @param source:
     *            source object
     * @param version:
     *            version of source which will be set to _CURRENT_VERSION file;
     *            if need to upload multiple versions by calling this method
     *            repeatedly, make sure the latest version is uploaded at last
     * @param fileNames:
     *            data file names (data files should be put under "sources"
     *            folder under "test/resources" directory)
     */
    protected void uploadSourceToS3(Source source, String version, List<String> fileNames) {
        try {
            String versionPath = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
            if (s3Service.isNonEmptyDirectory(s3Bucket, versionPath)) {
                s3Service.cleanupDirectory(s3Bucket, versionPath);
            }

            for (String fileName : fileNames) {
                InputStream fileStream = ClassLoader.getSystemResourceAsStream("sources/" + fileName);
                String objectKey = versionPath + "/" + fileName;
                s3Service.uploadInputStream(s3Bucket, objectKey, fileStream, true);
            }

            InputStream successStream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successObjectKey = versionPath + SUCCESS_FLAG;
            s3Service.uploadInputStream(s3Bucket, successObjectKey, successStream, true);

            InputStream currVersionStream = new ByteArrayInputStream(version.getBytes(StandardCharsets.UTF_8));
            String currVersionObjectKey = hdfsPathBuilder.constructVersionFile(source).toString();
            s3Service.uploadInputStream(s3Bucket, currVersionObjectKey, currVersionStream, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Prepare clean pod id on hdfs
     *
     * @param podId:
     *            pod id
     */
    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        this.podId = podId;
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Prepare clean pod id on hdfs and s3
     *
     * @param s3Bucket:
     *            s3 bucket
     * @param podId:
     *            pod id
     */
    protected void prepareCleanPod(String s3Bucket, String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        this.podId = podId;
        try {
            String podPath = hdfsPathBuilder.podDir().toString();
            HdfsUtils.rmdir(yarnConfiguration, podPath);
            if (s3Service.isNonEmptyDirectory(s3Bucket, podPath)) {
                s3Service.cleanupDirectory(s3Bucket, podPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void prepareCleanPod(Source source) {
        String podId = "Test" + source.getSourceName();
        prepareCleanPod(podId);
    }

    protected Iterator<GenericRecord> getGenericRecords(String path) {
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

    protected boolean isObjEquals(Object actual, Object expected) {
        if (actual == null && expected == null) {
            return true;
        }
        if (actual == null || expected == null) {
            return false;
        }
        if (actual instanceof Utf8) {
            actual = actual.toString();
        }
        return actual.equals(expected);
    }

    @SuppressWarnings("unused")
    private void dropJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }

    @SuppressWarnings("unused")
    private void truncateJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

}
