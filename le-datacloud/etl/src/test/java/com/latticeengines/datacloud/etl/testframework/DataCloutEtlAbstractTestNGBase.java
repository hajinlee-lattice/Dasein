package com.latticeengines.datacloud.etl.testframework;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;

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

    protected static final String SUCCESS_FLAG = "/_SUCCESS";

    @Value("${datacloud.test.env}")
    protected String testEnv;

    @Value("${datacloud.collection.host}")
    private String dbHost;

    @Value("${datacloud.collection.port}")
    private int dbPort;

    @Value("${datacloud.collection.db}")
    private String db;

    @Value("${datacloud.user}")
    private String dbUser;

    @Value("${datacloud.password.encrypted}")
    private String dbPassword;

    @Value("${datacloud.collection.sqoop.mapper.number:4}")
    private int numMappers;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

    @Autowired
    @Qualifier(value = "propDataBulkJdbcTemplate")
    protected JdbcTemplate jdbcTemplateBulkDB;

    protected String podId;

    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion) {
        InputStream baseAvroStream = ClassLoader
                .getSystemResourceAsStream("sources/" + baseSource.getSourceName() + ".avro");
        String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("part-0000.avro")
                .toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("_SUCCESS")
                    .toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceFile(Source baseSource, String baseSourceFile, String baseSourceVersion) {
        InputStream baseSourceStream = null;
        String targetPath = null;
        String successPath = null;
        if (baseSource instanceof IngestionSource) {
            baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSourceFile);
            targetPath = hdfsPathBuilder
                    .constructIngestionDir(((IngestionSource) baseSource).getIngestionName(), baseSourceVersion)
                    .append("/" + baseSourceFile).toString();
            successPath = hdfsPathBuilder
                    .constructIngestionDir(((IngestionSource) baseSource).getIngestionName(), baseSourceVersion)
                    .append("_SUCCESS").toString();
        } else {
            baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSourceFile + ".avro");
            targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion).append("part-0000.avro")
                    .toString();
            successPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion).append("_SUCCESS")
                    .toString();
        }

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
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceFile(String baseSource, String baseSourceFile, String baseSourceVersion) {
        InputStream baseSourceStream = null;
        String targetPath = null;
        String successPath = null;
        baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSourceFile + ".avro");
        targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("part-0000.avro")
                .toString();
        successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("_SUCCESS")
                .toString();
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
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void uploadBaseSourceData(String baseSource, String baseSourceVersion,
                                        List<Pair<String, Class<?>>> schema, Object[][] data) {
        String targetDir = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                .toString();
        String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                .append("_SUCCESS").toString();
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "part-00000.avro");
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    protected void extractSchema(Source source, String version, String avroDir) throws Exception {
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

    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        this.podId = podId;
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void prepareCleanPod(Source source) {
        String podId = "Test" + source.getSourceName();
        prepareCleanPod(podId);
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
