package com.latticeengines.aws.s3.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.conditions.StringCondition;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.util.Md5Utils;
import com.latticeengines.aws.s3.S3KeyFilter;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroStreamsIterator;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class S3ServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(S3ServiceImplTestNG.class);

    private static String DROP_FOLDER = "dropfolder";

    private static final String SOURCE_S3_BUCKET = "latticeengines-test-artifacts";

    @Inject
    private S3Service s3Service;

    @Inject
    private AmazonS3 s3Client;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.test.s3.bucket}")
    private String testBucket;

    @Value("${aws.customer.account.id}")
    private String accountId;

    @Value("${aws.test.customer.access.key}")
    private String customerAccessKey;

    @Value("${aws.test.customer.secret.key.encrypted}")
    private String customerSecret;

    private String dropBoxId;
    private String dropBoxDir;

    private AmazonS3 customerS3;

    @BeforeClass(groups = "functional")
    public void setup() {
        dropBoxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        dropBoxDir = DROP_FOLDER + "/" + dropBoxId;
        if (!s3Service.objectExist(testBucket, DROP_FOLDER)) {
            s3Service.createFolder(testBucket, DROP_FOLDER);
        }
        BasicAWSCredentialsProvider credentialsProvider = //
                new BasicAWSCredentialsProvider(customerAccessKey, customerSecret);
        customerS3 = AmazonS3ClientBuilder.standard() //
                .withCredentials(credentialsProvider).withRegion(awsRegion).build();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        s3Service.cleanupPrefix(testBucket, dropBoxDir);
        s3Client.deleteBucketPolicy(testBucket);
    }

    @Test(groups = "functional")
    public void testListSubFolders() {
        List<String> subFolders = Arrays.asList("folder1", "folder2", "folder3");
        List<String> secondaryFolders = Arrays.asList("sub1", "sub2", "sub3");
        subFolders.forEach(folder -> s3Service.createFolder(testBucket, dropBoxDir + "/" + folder));
        secondaryFolders.forEach(folder -> s3Service.createFolder(testBucket, dropBoxDir + "/folder3/" + folder));
        List<String> rootSubFolders = s3Service.listSubFolders(testBucket, dropBoxDir);
        Assert.assertNotNull(rootSubFolders);
        Assert.assertEquals(rootSubFolders.size(), 3);
        Assert.assertTrue(rootSubFolders.containsAll(subFolders));
        List<String> secondarySubFolders = s3Service.listSubFolders(testBucket, dropBoxDir + "/folder3");
        Assert.assertNotNull(secondarySubFolders);
        Assert.assertEquals(secondarySubFolders.size(), 3);
        Assert.assertTrue(secondarySubFolders.containsAll(secondaryFolders));

    }

    @Test(groups = "functional", enabled = false)
    public void testUploadMultiPart() {
        long MB = 1024 * 1024;

        String destKey1 = "uploadMultipartStreamTest/1M.txt";
        String destKey2 = "uploadMultipartStreamTest/20M.txt";

        char[] allA = new char[(int)(MB)];
        Arrays.fill(allA, 'a');
        String oneM = new String(allA);
        InputStream oneMStream = new ByteArrayInputStream(oneM.getBytes());
        s3Service.uploadInputStreamMultiPart(testBucket, destKey1, oneMStream, MB);

        char[] allB = new char[(int)(20 * MB)];
        for (int i = 0; i < 20; i++) {
            char x = (char)('a' + i);
            Arrays.fill(allB, (int)(i * MB), (int)((i + 1) * MB - 1), x);
        }
        String twentyM = new String(allB);
        InputStream twentyMStream = new ByteArrayInputStream(twentyM.getBytes());
        s3Service.uploadInputStreamMultiPart(testBucket, destKey2, twentyMStream, 20 * MB);
    }

    @Test(groups = "functional")
    public void testCopyLargeObjectAndTag() throws IOException {
        String sourceKey = "le-serviceapps/cdl/end2end/large_csv/1/Accounts.csv";
        String destKey = "copyLargeObjectTest/Accounts.csv";
        if (!s3Service.objectExist(SOURCE_S3_BUCKET, sourceKey)) {
            //skip test if file is not there.
            return;
        }
        if (s3Service.objectExist(testBucket, destKey)) {
            s3Service.cleanupPrefix(testBucket, destKey);
            Assert.assertFalse(s3Service.objectExist(testBucket, destKey));
        }
        s3Service.copyLargeObjects(SOURCE_S3_BUCKET, sourceKey, testBucket, destKey);
        Assert.assertTrue(s3Service.objectExist(testBucket, destKey));
        //Add Tag
        s3Service.addTagToObject(testBucket, destKey, "TestTag1", "This is tag 1");
        List<Tag> tagList = s3Service.getObjectTags(testBucket, destKey);
        Assert.assertNotNull(tagList);
        Assert.assertEquals(tagList.size(), 1);
        Assert.assertEquals(tagList.get(0).getKey(), "TestTag1");
        Assert.assertEquals(tagList.get(0).getValue(), "This is tag 1");
        s3Service.addTagToObject(testBucket, destKey, "TestTag1", "This is tag 2");
        tagList = s3Service.getObjectTags(testBucket, destKey);
        Assert.assertEquals(tagList.size(), 1);
        Assert.assertEquals(tagList.get(0).getKey(), "TestTag1");
        Assert.assertEquals(tagList.get(0).getValue(), "This is tag 2");
        s3Service.addTagToObject(testBucket, destKey, "TestTag2", "This is tag 2");
        tagList = s3Service.getObjectTags(testBucket, destKey);
        Assert.assertEquals(tagList.size(), 2);

        ObjectMetadata sourceMeta = s3Client.getObjectMetadata(SOURCE_S3_BUCKET, sourceKey);
        ObjectMetadata destMeta = s3Client.getObjectMetadata(testBucket, destKey);
        Assert.assertEquals(sourceMeta.getContentLength(), destMeta.getContentLength());
        try (InputStream destSteam = s3Service.readObjectAsStream(testBucket, destKey)) {
            String destMd5 = Md5Utils.md5AsBase64(destSteam);
            try (InputStream sourceSteam = s3Service.readObjectAsStream(SOURCE_S3_BUCKET, sourceKey)) {
                String sourceMd5 = Md5Utils.md5AsBase64(sourceSteam);
                Assert.assertEquals(destMd5, sourceMd5);
            }
        }
    }

    @Test(groups = "manual", enabled = false)
    public void testChangeKey() {
        String tgtKey = "0e9daa04-1400-4e55-88c5-b238a9d01721";
        s3Service.changeKeyRecursive("latticeengines-test-dev",
                "test_kms/distcp",
                "test_kms/distcp_encrypted",
                tgtKey);
        s3Service.changeKeyRecursive("latticeengines-test-dev",
                "test_kms/distcp_encrypted",
                "test_kms/distcp_encrypted_2",
                tgtKey);
        s3Service.changeKeyRecursive("latticeengines-test-dev",
                "test_kms/distcp_encrypted",
                "test_kms/distcp_unencrypted",
                "");
    }

    @Test(groups = "functional", dataProvider = "S3AvroIteratorData")
    public void testS3AvroRecordIterator(String[] data) {
        String prefix = this.getClass().getSimpleName() + "/TestS3AvroRecordIterator";
        String columnName = "TestColumn";
        uploadAvroIterDataToS3(prefix, columnName, data);
        Iterator<InputStream> streamIter = s3Service.getObjectStreamIterator(testBucket, prefix, new S3KeyFilter() {
        });
        List<String> expected = Arrays.stream(data) //
                .filter(Objects::nonNull) //
                .flatMap(d -> Arrays.stream(d.split(""))) //
                .collect(Collectors.toList());
        int nRow = 0;
        try (AvroStreamsIterator avroIter = AvroUtils.iterateAvroStreams(streamIter)) {
            for (GenericRecord record : (Iterable<GenericRecord>) () -> avroIter) {
                Assert.assertNotNull(record);
                Assert.assertEquals(record.get(columnName).toString(), "A");
                nRow++;
            }
        }
        Assert.assertEquals(nRow, expected.size());
        clearS3Prefix(prefix);
    }

    private void clearS3Prefix(String prefix) {
        if (s3Service.isNonEmptyDirectory(testBucket, prefix)) {
            s3Service.cleanupPrefix(testBucket, prefix);
        }
    }

    @Test(groups = "functional")
    public void testUploadLocalDirectory() {
        String dirName = "TestUploadLocalDirectory";
        String prefix = this.getClass().getSimpleName() + "/" + dirName;
        String columnName = "TestColumn";
        String[] data = new String[]{"A", "AA", "BB", "AA", "BB", "CC", "CC", "DD", "EE", "FF", "FF", "HH"};
        uploadLocalAvroToS3(prefix, dirName, columnName, data);
        List<S3ObjectSummary> s3ObjectSummaries = s3Service.listObjects(testBucket, prefix);
        Assert.assertEquals(data.length, s3ObjectSummaries.size());
        long sizeOnS3 = s3ObjectSummaries.stream().mapToLong(s3ObjectSummary -> s3ObjectSummary.getSize()).sum();
        File dir = new File(dirName);
        long sizeOnLocal = Arrays.stream(dir.listFiles()).mapToLong(file -> file.length()).sum();
        Assert.assertEquals(sizeOnS3, sizeOnLocal);
        deleteDir(dir);
        clearS3Prefix(prefix);
    }

    // Every 1-d ARRAY is a test case.
    // Every STRING is a file. Null string is empty file.
    // Every LETTER ("A") is an avro record in the file (single column
    // "TestColumn" with value as "A")
    @DataProvider(name = "S3AvroIteratorData")
    private Object[][] getS3AvroIteratorData() {
        return new Object[][]{
                // Single file with single row
                {new String[]{"A"}}, //
                // Multiple files with multiple rows
                {new String[]{"AA", "A", "A"}}, //
                {new String[]{"A", "AA", "A"}}, //
                {new String[]{"A", "A", "AA"}}, //
                {new String[]{"AAA", "AAA", "AAA"}}, //
                // Single empty file
                {new String[]{null}}, //
                // All empty files
                {new String[]{null, null, null}}, //
                // Some empty files
                {new String[]{"AA", "A", null}}, //
                {new String[]{"A", null, null, "AA"}}, //
                {new String[]{null, "AA", "A"}}, //
        };
    }

    private List<GenericRecord> generateRecords(Schema schema, String columnName, String data) {
        List<GenericRecord> records = new ArrayList<>();
        if (data != null) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            String content = data;
            for (int j = 0; j < content.length(); j++) {
                builder.set(columnName, content.substring(j, j + 1));
                records.add(builder.build());
            }
        }
        return records;
    }

    private void writeData(Schema schema, List<GenericRecord> records, OutputStream out) {
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
            try (DataFileWriter<GenericRecord> creator = writer.create(schema, out)) {
                for (GenericRecord datum : records) {
                    try {
                        creator.append(datum);
                    } catch (Exception e) {
                        log.error("Data for the error row: " + datum.toString());
                        throw new IOException(e);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to convert GenericRecord to OutputStream", e);
        }
    }

    private List<Pair<String, Class<?>>> getColumns(String columnName) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(columnName, String.class));
        return columns;
    }

    private void uploadAvroIterDataToS3(String prefix, String columnName, String[] data) {
        List<Pair<String, Class<?>>> columns = getColumns(columnName);
        clearS3Prefix(prefix);
        for (int i = 0; i < data.length; i++) {
            String fileName = "File_" + i;
            Schema schema = AvroUtils.constructSchema(fileName, columns);
            List<GenericRecord> records = generateRecords(schema, columnName, data[i]);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeData(schema, records, out);
            String objectKey = prefix + "/" + fileName + ".avro";
            s3Service.uploadInputStream(testBucket, objectKey, new ByteArrayInputStream(out.toByteArray()), true);
            log.info("Uploaded data {} to S3 object {}", data[i], objectKey);
        }
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                deleteDir(new File(dir, children[i]));
            }
        }
        dir.delete();
    }

    private void makeDir(String dirName) {
        File dir = new File(dirName);
        deleteDir(dir);
        dir.mkdir();
    }

    private void uploadLocalAvroToS3(String prefix, String dirName, String columnName, String[] data) {
        List<Pair<String, Class<?>>> columns = getColumns(columnName);
        clearS3Prefix(prefix);
        try {
            makeDir(dirName);
            for (int i = 0; i < data.length; i++) {
                String fileName = "TestFile_" + UUID.randomUUID().toString().replace("-", "");
                Schema schema = AvroUtils.constructSchema(fileName, columns);
                List<GenericRecord> records = generateRecords(schema, columnName, data[i]);
                File file = new File(dirName, fileName + ".avro");
                FileOutputStream out = new FileOutputStream(file);
                writeData(schema, records, out);
            }
            File directory = new File(dirName);
            s3Service.uploadLocalDirectory(testBucket, prefix, directory.getCanonicalPath(), true);
        } catch (IOException e) {
            throw new RuntimeException("Fail to upload avro files to š3", e);
        }
    }

    private Policy getCustomerPolicy(String dropBoxId, String accountId) {
        String bucketPolicy = s3Service.getBucketPolicy(testBucket);
        List<Statement> statements = new ArrayList<>();
        Policy policy;
        if (StringUtils.isBlank(bucketPolicy)) {
            policy = new Policy();
        } else {
            policy = Policy.fromJson(bucketPolicy);
            revokeAccountFromDropBox(policy, dropBoxId, accountId);
        }
        boolean hasAccountStmt = false;
        if (CollectionUtils.isNotEmpty(policy.getStatements())) {
            for (Statement stmt: policy.getStatements()) {
                if (stmt.getId().equals(accountId)) {
                    insertAccountStatement(testBucket, dropBoxId, stmt);
                    hasAccountStmt = true;
                }
                statements.add(stmt);
            }
        }
        if (!hasAccountStmt) {
            statements.add(getAccountStatement(testBucket, dropBoxId, accountId));
        }
        statements.add(getAccountListDropBoxStatement(testBucket, dropBoxId, accountId));
        policy.setStatements(statements);
        return policy;
    }

    private Statement getAccountStatement(String bucketName, String dropBoxId, String accountId) {
        String arn = "arn:aws:s3:::" + bucketName + "/" + DROP_FOLDER + "/" + dropBoxId;
        return new Statement(Statement.Effect.Allow) //
                        .withId(accountId) //
                        .withPrincipals(new Principal(accountId)) //
                        .withActions(//
                                S3Actions.AbortMultipartUpload, //
                                S3Actions.GetObject, //
                                S3Actions.PutObject, //
                                S3Actions.DeleteObject, //
                                S3Actions.SetObjectAcl
                        ) //
                        .withResources(new Resource(arn + "*"));
    }

    private Statement getAccountListDropBoxStatement(String bucketName, String dropBoxId, String accountId) {
        return new Statement(Statement.Effect.Allow) //
                .withId(accountId + "_" + dropBoxId) //
                .withPrincipals(new Principal(accountId)) //
                .withActions(S3Actions.ListObjects) //
                .withResources(new Resource("arn:aws:s3:::" + bucketName))
                .withConditions(new StringCondition(//
                        StringCondition.StringComparisonType.StringLike, //
                        "s3:prefix", //
                        DROP_FOLDER + "/" + dropBoxId + "*" //
                ));
    }

    private void revokeAccountFromDropBox(Policy policy, String dropBoxId, String accountId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    if (accountId.equals(stmt.getId())) {
                        List<Resource> resourceList = stmt.getResources().stream() //
                                .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                                .collect(Collectors.toList());
                        stmt.setResources(resourceList);
                    }
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(accountId) && stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private void revokeDropBoxFromDropBox(Policy policy, String dropBoxId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    List<Resource> resourceList = stmt.getResources().stream() //
                            .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                            .collect(Collectors.toList());
                    stmt.setResources(resourceList);
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private void insertAccountStatement(String bucketName, String dropBoxId, Statement statement) {
        String arn = "arn:aws:s3:::" + bucketName + "/" + DROP_FOLDER + "/" + dropBoxId;
        List<Resource> rscs = new ArrayList<>(statement.getResources());
        rscs.add(new Resource(arn));
        rscs.add(new Resource(arn + "*"));
        statement.setResources(rscs);
    }

    private void verifyAccess() {
        customerS3.doesObjectExist(testBucket, dropBoxDir);
        customerS3.listObjectsV2(testBucket, dropBoxDir);
    }

    private void verifyNoAccess() {
        try {
            customerS3.getObject(testBucket, dropBoxDir);
            Assert.fail("Should throw AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("403"), e.getMessage());
        }
        try {
            customerS3.listObjectsV2(testBucket, dropBoxDir);
            Assert.fail("Should throw AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("403"), e.getMessage());
        }
    }
}
