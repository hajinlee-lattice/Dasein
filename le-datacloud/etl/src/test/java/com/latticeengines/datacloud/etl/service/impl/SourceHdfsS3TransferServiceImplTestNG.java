package com.latticeengines.datacloud.etl.service.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.Tag;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.Path;

public class SourceHdfsS3TransferServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SourceHdfsS3TransferServiceImplTestNG.class);

    @Inject
    private S3Service s3Service;

    @Inject
    private SourceHdfsS3TransferServiceImpl sourceHdfsS3TransferService;

    private static final String POD = SourceHdfsS3TransferServiceImplTestNG.class.getSimpleName()
            + UUID.randomUUID().toString();
    private static final String S3_FILE_PREFIX = "S3_";
    private static final String SOURCE = "BomboraFirehose";
    private static final IngestionSource INGESTION_SOURCE = new IngestionSource(SOURCE);
    private static final GeneralSource GENERAL_SOURCE = new GeneralSource(SOURCE);
    private static final GeneralSource GENERAL_SOURCE_2 = new GeneralSource(SOURCE + "_2");
    private static final String CURR_VER = "2019-01-01_00-00-00_UTC";
    private static final String PREV_VER = "2018-01-01_00-00-00_UTC";
    private static final List<String> INGESTION_SOURCE_FILES = Arrays.asList( //
            "AllDomainsAllTopicsZips_20190714_1.csv.gz", "AllDomainsAllTopicsZips_20190714_2.csv.gz");
    private static final List<String> INGESTION_SOURCE_FILES_S3 = INGESTION_SOURCE_FILES.stream()
            .map(file -> S3_FILE_PREFIX + file).collect(Collectors.toList());
    private static final List<String> GENERAL_SOURCE_FILES = Arrays.asList( //
            "BomboraDepivoted_20160930.avro", "BomboraDepivoted_20161001.avro");
    private static final List<String> GENERAL_SOURCE_FILES_S3 = GENERAL_SOURCE_FILES.stream()
            .map(file -> S3_FILE_PREFIX + file).collect(Collectors.toList());
    private static final List<Pair<String, String>> TAGS = Arrays.asList(//
            Pair.of("Tag1", "Value1"), Pair.of("Tag2", "Value2"));


    @BeforeClass(groups = "pipeline1")
    public void setup() throws Exception {
        prepareCleanPod(s3Bucket, POD);
    }

    @AfterClass(groups = "pipeline1")
    public void destroy() {
        prepareCleanPod(s3Bucket, POD);
    }

    /**
     * Test logic:
     * 1. Prepare ingestion source on hdfs
     * 2. Copy from hdfs to s3 -- verify success
     * 3. Copy from hdfs to s3 again with failIfExisted = true -- verify fail
     * 4. Rename files on s3
     * 5. Copy from s3 back to hdfs -- verify success (existed files on hdfs should be deleted)
     * 6. Copy from s3 back to hdfs again with failIfExisted = true -- verify fail
     */
    @Test(groups = "pipeline1")
    public void testIngestionSource() {
        uploadSourceToHdfs(INGESTION_SOURCE, CURR_VER, INGESTION_SOURCE_FILES, false);

        sourceHdfsS3TransferService.transfer(true, INGESTION_SOURCE, CURR_VER, TAGS, false, false);
        verifySourceOnS3(INGESTION_SOURCE, CURR_VER, CURR_VER, new HashSet<>(INGESTION_SOURCE_FILES), TAGS, false);
        Assert.assertThrows(RuntimeException.class,
                () -> sourceHdfsS3TransferService.transfer(true, INGESTION_SOURCE, CURR_VER, null, false, true));

        renameS3Files(INGESTION_SOURCE, CURR_VER, INGESTION_SOURCE_FILES);

        sourceHdfsS3TransferService.transfer(false, INGESTION_SOURCE, CURR_VER, null, false, false);
        verifySourceOnHdfs(INGESTION_SOURCE, CURR_VER, CURR_VER, new HashSet<>(INGESTION_SOURCE_FILES_S3), false);
        Assert.assertThrows(RuntimeException.class,
                () -> sourceHdfsS3TransferService.transfer(false, INGESTION_SOURCE, CURR_VER, null, false, true));
    }

    /**
     * Test logic:
     * 1. Prepare general source on hdfs
     * 2. Copy from hdfs to s3 -- verify success
     * 3. Copy from hdfs to s3 again with failIfExisted = true -- verify fail
     * 4. Rename files on s3
     * 5. Copy from s3 back to hdfs -- verify success (existed files on hdfs should be deleted)
     * 6. Copy from s3 back to hdfs again with failIfExisted = true -- verify fail
     */
    @Test(groups = "functional")
    public void testGeneralSource() {
        uploadSourceToHdfs(GENERAL_SOURCE, CURR_VER, GENERAL_SOURCE_FILES, true);

        sourceHdfsS3TransferService.transfer(true, GENERAL_SOURCE, CURR_VER, TAGS, false, false);
        verifySourceOnS3(GENERAL_SOURCE, CURR_VER, CURR_VER, new HashSet<>(GENERAL_SOURCE_FILES), TAGS, true);
        Assert.assertThrows(RuntimeException.class,
                () -> sourceHdfsS3TransferService.transfer(true, GENERAL_SOURCE, CURR_VER, null, false, true));

        renameS3Files(GENERAL_SOURCE, CURR_VER, GENERAL_SOURCE_FILES);

        sourceHdfsS3TransferService.transfer(false, GENERAL_SOURCE, CURR_VER, null, false, false);
        verifySourceOnHdfs(GENERAL_SOURCE, CURR_VER, CURR_VER, new HashSet<>(GENERAL_SOURCE_FILES_S3), true);
        Assert.assertThrows(RuntimeException.class,
                () -> sourceHdfsS3TransferService.transfer(false, GENERAL_SOURCE, CURR_VER, null, false, true));
    }

    /**
     * Test logic:
     * 1. Prepare general source without schema, and with 2 versions (CURR_VER & PREV_VER) on hdfs
     * 2. Prepare general source without schema, and with CURR_VER version on s3
     * 2. Copy PREV_VER from hdfs to s3 with tagging -- verify success
     *    (_CURRENT_VERSION on s3 should not be updated)
     * 4. Rename files on s3
     * 5. Copy PREV_VER from s3 back to hdfs -- verify success
     *    (existed files on hdfs should be deleted; _CURRENT_VERSION on hdfs should not be updated)
     */
    @Test(groups = "functional")
    public void testGeneralSource2() {
        uploadSourceToHdfs(GENERAL_SOURCE_2, PREV_VER, GENERAL_SOURCE_FILES, false);
        uploadSourceToHdfs(GENERAL_SOURCE_2, CURR_VER, GENERAL_SOURCE_FILES, false);
        uploadSourceToS3(GENERAL_SOURCE_2, CURR_VER, GENERAL_SOURCE_FILES);

        sourceHdfsS3TransferService.transfer(true, GENERAL_SOURCE_2, PREV_VER, TAGS, false, false);
        verifySourceOnS3(GENERAL_SOURCE_2, PREV_VER, CURR_VER, new HashSet<>(GENERAL_SOURCE_FILES), TAGS, false);

        renameS3Files(GENERAL_SOURCE_2, PREV_VER, GENERAL_SOURCE_FILES);

        sourceHdfsS3TransferService.transfer(false, GENERAL_SOURCE_2, PREV_VER, null, false, false);
        verifySourceOnHdfs(GENERAL_SOURCE_2, PREV_VER, CURR_VER, new HashSet<>(GENERAL_SOURCE_FILES_S3), false);
    }

    /**
     * After files are uploaded to s3, add a prefix to file names so that when
     * they are copied back to hdfs, they could be differentiated with original
     * files on hdfs
     *
     * @param source:
     *            testing source object
     * @param version:
     *            version of testing source
     * @param files:
     *            data files of testing source
     */
    private void renameS3Files(Source source, String version, List<String> files) {
        String dir = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
        files.forEach(file -> {
            String objectKey = dir + "/" + file;
            String newObjectKey = dir + "/" + S3_FILE_PREFIX + file;
            s3Service.moveObject(s3Bucket, objectKey, s3Bucket, newObjectKey);
        });
    }

    /**
     * Verify source copied to s3: schema file, data files, _CURRENT_VERSION
     * file, tags
     *
     * @param source:
     *            testing source object
     * @param version:
     *            version of copied source
     * @param expectedCurrVer:
     *            expected value in _CURRENT_VERSION file
     * @param expectedFiles:
     *            expected data files copied
     * @param expectedTags:
     *            expected tags on s3 objects
     * @param hasSchema:
     *            whether schema file should exist
     */
    private void verifySourceOnS3(Source source, String version, String expectedCurrVer, Set<String> expectedFiles,
            List<Pair<String, String>> expectedTags, boolean hasSchema) {
        // Verify schema file
        Path schemaPath = hdfsPathBuilder.constructSchemaFile(source, version);
        if (hasSchema) {
            Assert.assertTrue(s3Service.objectExist(s3Bucket, schemaPath.toString()),
                    schemaPath.toString() + " doesn't exist on S3: ");
            verifyTags(s3Service.getObjectTags(s3Bucket, schemaPath.toString()), expectedTags);
        } else {
            Assert.assertTrue(schemaPath == null || !s3Service.objectExist(s3Bucket, schemaPath.toString()),
                    schemaPath == null ? "" : schemaPath.toString() + " shouldn't exist on S3: ");
        }

        // Verify data files
        String dataPath = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
        Assert.assertTrue(s3Service.isNonEmptyDirectory(s3Bucket, dataPath),
                "S3 " + dataPath + " doesn't have files under it: ");
        List<String> filePaths = s3Service.getFilesForDir(s3Bucket, getPathForListingFile(dataPath));
        List<String> files = filePaths.stream().filter(filePath -> !filePath.endsWith(HdfsPathBuilder.SUCCESS_FILE))
                .map(filePath -> filePath.substring(filePath.lastIndexOf("/") + 1)).collect(Collectors.toList());
        Assert.assertEquals((new HashSet<>(files)).size(), expectedFiles.size(),
                "Number of files under S3 path " + dataPath + " is not same as expected: ");
        files.forEach(file -> {
            Assert.assertTrue(expectedFiles.contains(file), "S3 file " + file + " is not expected: ");
        });
        filePaths.forEach(filePath -> {
            verifyTags(s3Service.getObjectTags(s3Bucket, filePath), expectedTags);
        });
        String successPath = dataPath + "/" + HdfsPathBuilder.SUCCESS_FILE;
        Assert.assertTrue(s3Service.objectExist(s3Bucket, successPath), successPath + " doesn't exist on S3: ");
        verifyTags(s3Service.getObjectTags(s3Bucket, successPath), expectedTags);

        // Verify _CURRENT_VERSION file
        String currVerPath = hdfsPathBuilder.constructVersionFile(source).toString();

        try {
            String currVer = IOUtils.toString(s3Service.readObjectAsStream(s3Bucket, currVerPath),
                    Charset.defaultCharset());
            Assert.assertEquals(currVer, expectedCurrVer, "Current version of "
                    + sourceHdfsS3TransferService.getSourceNameForLogging(source) + " is not same as expected: ");
        } catch (IOException e) {
            Assert.fail(e.getMessage(), e);
        }
        if (expectedCurrVer.equals(version)) {
            verifyTags(s3Service.getObjectTags(s3Bucket, currVerPath), expectedTags);
        } else {
            verifyTags(s3Service.getObjectTags(s3Bucket, currVerPath), null);
        }
    }

    /**
     * Verify source copied to hdfs: schema file, data files, _CURRENT_VERSION
     * file
     *
     * @param source:
     *            testing source object
     * @param version:
     *            version of copied source
     * @param expectedCurrVer:
     *            expected value in _CURRENT_VERSION file
     * @param expectedFiles:
     *            expected data files copied
     * @param hasSchema:
     *            whether schema file should exist
     */
    private void verifySourceOnHdfs(Source source, String version, String expectedCurrVer, Set<String> expectedFiles,
            boolean hasSchema) {
        // Verify schema file
        Path schemaPath = hdfsPathBuilder.constructSchemaFile(source, version);
        try {
            if (hasSchema) {
                Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, schemaPath.toString()),
                        schemaPath + " doesn't exist on HDFS: ");
            } else {
                Assert.assertTrue(
                        schemaPath == null || !HdfsUtils.fileExists(yarnConfiguration, schemaPath.toString()),
                        schemaPath + " shouldn't exist on HDFS: ");
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage(), e);
        }

        // Verify data files
        String dataPath = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
        try {
            Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, dataPath),
                    dataPath + " doesn't exist on HDFS: ");
        } catch (IOException e) {
            Assert.fail(e.getMessage(), e);
        }
        HdfsFileFilter filter = new HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
                return !HdfsPathBuilder.SUCCESS_FILE.equals(fileStatus.getPath().getName());
            }
        };
        try {
            List<String> files = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, dataPath, filter);
            Assert.assertEquals((new HashSet<>(files)).size(), expectedFiles.size(),
                    "Number of files under HDFS path " + dataPath + " is not same as expected: ");
            files.forEach(file -> {
                Assert.assertTrue(expectedFiles.contains(file.substring(file.lastIndexOf("/") + 1)),
                        file + " is not expected: ");
            });
        } catch (IOException e) {
            Assert.fail(e.getMessage(), e);
        }
        String successPath = dataPath + "/" + HdfsPathBuilder.SUCCESS_FILE;
        try {
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, successPath),
                    successPath + " doesn't exist on HDFS: ");
        } catch (IOException e) {
            Assert.fail(e.getMessage(), e);
        }

        // Verify _CURRENT_VERSION file
        Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(source), expectedCurrVer,
                "Current version of " + sourceHdfsS3TransferService.getSourceNameForLogging(source)
                        + " is not same as expected: ");
    }

    private void verifyTags(List<Tag> actualTags, List<Pair<String, String>> expectedTags) {
        if (CollectionUtils.isEmpty(expectedTags)) {
            Assert.assertTrue(CollectionUtils.isEmpty(actualTags));
            return;
        }
        Map<String, String> actualMap = actualTags.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));
        Map<String, String> expectedMap = expectedTags.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        Assert.assertEquals(actualMap.size(), expectedMap.size());
        actualMap.forEach((key, value) -> {
            Assert.assertEquals(actualMap.get(key), expectedMap.get(key));
        });
    }

    private String getPathForListingFile(String path) {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
