package com.latticeengines.matchapi.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.yarn.client.YarnClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.matchapi.testframework.TestMatchInputService;
import com.latticeengines.matchapi.testframework.TestMatchInputUtils;
import com.latticeengines.yarn.exposed.service.JobService;

@Component
public class MatchResourceDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final String avroDir = "/tmp/MatchResourceDeploymentTestNG";
    private static final String fileName = "SourceFile_csv.avro";
    private static final String podId = "MatchResourceDeploymentTestNG";
    private static List<String> domains = new ArrayList<>();
    private static final Logger log = LoggerFactory.getLogger(MatchResourceDeploymentTestNG.class);

    static {
        domains.add("fb.com");
        domains.add("google.com");
        domains.add("salesforce.com");
        domains.add("microsoft.com");
        domains.add("yahoo.com");
        domains.add("dnb.com");
        domains.add("wikipedia.com");
        domains.add("amazon.com");
        domains.add("gmail.com");
        domains.add("apple.com");
        domains.add("apache.com");
    }

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Autowired
    private PublicDomainService publicDomainService;

    @Value("${datacloud.match.latest.data.cloud.major.version}")
    private String latestMajorVersion;

    @Autowired
    private YarnClient yarnClient;

    @Autowired
    private JobService jobService;

    @BeforeClass(groups = "deployment")
    private void setup() {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDir);
    }

    @AfterClass(groups = "deployment")
    private void destroy() {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDir);
    }

    // Test against DerivedColumnsCache
    @Test(groups = "deployment", enabled = true)
    public void testPredefined() {
        List<List<Object>> data = TestMatchInputUtils.getGoodInputData();
        MatchInput input = testMatchInputService.prepareSimpleRTSMatchInput(data);
        input.setDataCloudVersion("1.0.0");
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "deployment", enabled = true)
    public void testAccountMasterRTSMatch() {
        testAccountMasterRTSMatch(true, "a@fb.com", true, false, null);
        testAccountMasterRTSMatch(true, "a@salesforce.com", false, true, "null");
        testAccountMasterRTSMatch(true, "a@dnb.com", true, true, "NULL");
        // testAccountMasterRTSMatch(true, null, false, true, "079942718");
    }

    private void testAccountMasterRTSMatch(boolean resolveKeyMap, String domain, boolean isEmail,
                                           boolean setUnionSelection, String duns) {
        MatchInput input = prepareSimpleMatchInput(resolveKeyMap, domain, isEmail, setUnionSelection, duns);
        String latestVersion = dataCloudVersionEntityMgr.currentApprovedVersion().getVersion();
        input.setDataCloudVersion(latestVersion);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        int idx = 0;
        for (Object res : output.getResult().get(0).getOutput()) {
            String field = output.getOutputFields().get(idx++);
            if (!StringUtils.isEmpty(res)) {
                System.out.print(field + " = " + res + ", ");
            }
        }
    }

    @Test(groups = "deployment", dataProvider = "latestDataCloudVersions", enabled = true)
    public void testRealtimeBulkMatch(String version) throws IOException {
        int size = 200;
        List<MatchInput> inputList = prepareBulkMatchInput(size, version, true);

        BulkMatchInput input = new BulkMatchInput();
        input.setHomogeneous(false);
        input.setInputList(inputList);

        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(input));

        long startLookup = System.currentTimeMillis();
        BulkMatchOutput output = matchProxy.matchRealTime(input);
        System.out.println("Time taken to do dnb based AM lookup for " + size + " entries (with "
                + (size > domains.size() ? domains.size() : size) + " unique domains) = "
                + (System.currentTimeMillis() - startLookup) + " millis");
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getOutputList());
        Assert.assertEquals(output.getOutputList().size(), size);

        for (MatchOutput outputRecord : output.getOutputList()) {
            Assert.assertNotNull(outputRecord);
            Assert.assertTrue(outputRecord.getResult().size() > 0);
            if (!publicDomainService.isPublicDomain(outputRecord.getResult().get(0).getPreMatchDomain())) {
                Assert.assertTrue(outputRecord.getResult().get(0).isMatched(),
                        JsonUtils.serialize(outputRecord.getResult().get(0))
                                + " should match as it is not a public domain.");
            } else {
                // Domain-only public domain match without name/duns will be
                // treated as normal domain
                Assert.assertTrue(outputRecord.getResult().get(0).getErrorMessages().size() > 0);
                Assert.assertTrue(outputRecord.getResult().get(0).getErrorMessages().get(0).contains("public domain"));
            }
        }
    }

    @Test(groups = "deployment", dataProvider = "latestDataCloudVersions", enabled = true)
    public void testRealtimeMatchWithMultipleRecords(String version) throws IOException {
        int size = 200;
        MatchInput matchInput = prepareMatchInputWithMultipleRecords(size, version);

        long startLookup = System.currentTimeMillis();
        MatchOutput output = matchProxy.matchRealTime(matchInput);
        if (version.equals("1.0.0")) {
            System.out.println("Time taken to do DerivedColumnsCache lookup for " + size + " entries (with "
                    + (size > domains.size() ? domains.size() : size) + " unique domains) = "
                    + (System.currentTimeMillis() - startLookup) + " millis");
        } else {
            System.out.println("Time taken to do dnb based AM lookup for " + size + " entries (with "
                    + (size > domains.size() ? domains.size() : size) + " unique domains) = "
                    + (System.currentTimeMillis() - startLookup) + " millis");
        }
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), size);

        for (OutputRecord outputRecord : output.getResult()) {
            Assert.assertNotNull(outputRecord);
            Assert.assertTrue(outputRecord.getOutput().size() > 0);
            if (!publicDomainService.isPublicDomain(outputRecord.getPreMatchDomain())) {
                Assert.assertTrue(outputRecord.isMatched(),
                        outputRecord.getPreMatchDomain() + " should match as it is not a public domain.");
            } else {
                // Domain-only public domain match without name/duns will be
                // treated as normal domain
                Assert.assertTrue(outputRecord.getErrorMessages().size() > 0);
                Assert.assertTrue(outputRecord.getErrorMessages().get(0).contains("public domain"));
            }
        }
    }

    private MatchInput prepareMatchInputWithMultipleRecords(int size, String dataCloudVersion) {
        List<MatchInput> inputList = prepareBulkMatchInput(size, dataCloudVersion, true);

        MatchInput matchInput = inputList.get(0);
        List<List<Object>> inputDataList = matchInput.getData();

        for (MatchInput input : inputList) {
            if (input == matchInput) {
                continue;
            }

            inputDataList.add(input.getData().get(0));
        }

        matchInput.setNumRows(inputList.size());
        return matchInput;
    }

    private List<MatchInput> prepareBulkMatchInput(int count, String dataCloudVersion, boolean resolveKeyMap) {
        List<MatchInput> inputList = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            MatchInput input = prepareSimpleMatchInput(resolveKeyMap, //
                    domains.get(i % domains.size()), //
                    false, true, null);
            input.setDataCloudVersion(dataCloudVersion);
            inputList.add(input);
        }

        return inputList;
    }

    private MatchInput prepareSimpleMatchInput(boolean resolveKeyMap, String domain, boolean isEmail,
                                               boolean setUnionSelection, String duns) {
        Object[][] data = new Object[][] { { 0, domain, duns } };
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        MatchInput input = new MatchInput();

        if (setUnionSelection) {
            UnionSelection us = new UnionSelection();
            Map<Predefined, String> predefinedSelections = new HashMap<>();
            predefinedSelections.put(Predefined.RTS, "2.0");
            us.setPredefinedSelections(predefinedSelections);
            input.setUnionSelection(us);
        } else {
            input.setPredefinedSelection(Predefined.RTS);
        }
        input.setTenant(new Tenant("PD_Test"));
        List<String> fields = Arrays.asList("ID", isEmail ? "Email" : "Domain", "DUNS");
        input.setFields(fields);
        if (resolveKeyMap) {
            input.setKeyMap(MatchKeyUtils.resolveKeyMap(fields));
        }
        input.setData(mockData);
        input.setDataCloudVersion(dataCloudVersionEntityMgr.currentApprovedVersion().getVersion());
        return input;
    }

    @Test(groups = "deployment", enabled = true)
    public void testAutoResolvedKeyMap() {
        List<List<Object>> data = TestMatchInputUtils.getGoodInputData();
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);

        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "deployment", dataProvider = "recentApprovedVersions", enabled = true)
    public void testBulkMatchWithSchema(String version) throws Exception {
        log.info("DataCloud Version : " + version);
        String avroDirInThisRun = avroDir + "/" + version;
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDirInThisRun);
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.add(Integer.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        uploadDataCsv(avroDirInThisRun, fileName, "matchinput/BulkMatchInput.csv", fieldTypes, "ID");

        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroDirInThisRun + "/" + fileName));

        // use avro dir and with schema
        MatchInput input = createAvroBulkMatchInput(true, schema, version, avroDir, fileName);
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) input.getInputBuffer();
        avroInputBuffer.setAvroDir(avroDirInThisRun);
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ApplicationId.fromString(command.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
        Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);

        MatchCommand finalStatus = matchProxy.bulkMatchStatus(command.getRootOperationUid());
        Assert.assertEquals(finalStatus.getApplicationId(), appId.toString());
        Assert.assertEquals(finalStatus.getRootOperationUid(), command.getRootOperationUid());
        Assert.assertEquals(finalStatus.getProgress(), 1f);
        Assert.assertEquals(finalStatus.getMatchStatus(), MatchStatus.FINISHED);
        Assert.assertEquals(finalStatus.getResultLocation(),
                hdfsPathBuilder.constructMatchOutputDir(command.getRootOperationUid()).toString());
        Assert.assertEquals(finalStatus.getRowsMatched(), new Integer(101));

        List<GenericRecord> matchResults = AvroUtils.getDataFromGlob(
                yarnConfiguration, new Path(finalStatus.getResultLocation() + "/" + "*.avro").toString());
        for (GenericRecord record : matchResults) {
            Object value = record.get("HPAEmailSuffix");
            String valueStr = (value instanceof Utf8) ? value.toString() : (String) value;
            if (valueStr != null) {
                Assert.assertFalse(valueStr.contains("\r"));
                Assert.assertFalse(valueStr.contains("\n"));
                Assert.assertFalse(valueStr.contains("\r\n"));
            }
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testBulkMatchWithoutSchema() {
        String version = dataCloudVersionEntityMgr.latestApprovedForMajorVersion(latestMajorVersion).getVersion();

        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDir);
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.add(Integer.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        uploadDataCsv(avroDir, fileName, "matchinput/BulkMatchInput.csv", fieldTypes, "ID");

        // use avro file and without schema
        MatchInput input = createAvroBulkMatchInput(false, null, version, avroDir, fileName);
        input.setExcludePublicDomain(true);
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ApplicationId.fromString(command.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
        Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);
        Assert.assertEquals(matchCommand.getRowsMatched(), new Integer(100));
    }

    @Test(groups = "deployment", dataProvider = "recentApprovedVersions", enabled = true)
    public void testMultiBlockBulkMatch(String version) throws InterruptedException {
        HdfsPodContext.changeHdfsPodId(podId);
        String path = avroDir + "/" + version;
        cleanupAvroDir(path);
        uploadTestAVro(path, fileName);
        Long expectedTotal = AvroUtils.count(yarnConfiguration, new Path(path, fileName).toString());

        MatchInput input = createAvroBulkMatchInput(true, null, version, path, fileName);
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ApplicationId.fromString(command.getApplicationId());
        log.info("Test multi-block match command: DataCloudVersion = {}, ApplicationId = {}, Submitted Rows = {}",
                version, appId.toString(), expectedTotal);

        // mimic one block failed
        while (command.getMatchBlocks() == null || command.getMatchBlocks().isEmpty()) {
            Thread.sleep(1000L);
            command = matchProxy.bulkMatchStatus(command.getRootOperationUid());
        }
        String blockAppId = command.getMatchBlocks().get(0).getApplicationId();
        // Kill one block and expect it will be retried automatically
        jobService.killJob(ApplicationId.fromString(blockAppId));

        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
        Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);

        MatchCommand finalStatus = matchProxy.bulkMatchStatus(command.getRootOperationUid());
        Assert.assertEquals(finalStatus.getApplicationId(), appId.toString());
        Assert.assertEquals(finalStatus.getRootOperationUid(), command.getRootOperationUid());
        Assert.assertEquals(finalStatus.getProgress(), 1f);
        Assert.assertEquals(finalStatus.getMatchStatus(), MatchStatus.FINISHED);
        Assert.assertEquals(finalStatus.getResultLocation(),
                hdfsPathBuilder.constructMatchOutputDir(command.getRootOperationUid()).toString());
        Assert.assertEquals(AvroUtils.count(yarnConfiguration, finalStatus.getResultLocation() + "/*.avro"),
                expectedTotal);
    }

    @Test(groups = "deployment", enabled = true)
    public void testGetBulkConfig() {
        String currentVersion = dataCloudVersionEntityMgr.currentApprovedVersionAsString();
        MatchInput input = createAvroBulkMatchInput(true, null, currentVersion, avroDir, fileName);
        BulkMatchWorkflowConfiguration bulkConf = matchProxy.getBulkConfig(input, podId);
        Assert.assertNotNull(bulkConf);
        Assert.assertTrue(bulkConf.getSwpkgNames().contains("datacloud"));
    }

    @DataProvider(name = "recentApprovedVersions", parallel = true)
    public Object[][] recentApprovedVersions() {
        Set<String> distinctMajorVer = new HashSet<>();
        List<String> prevAndCurrentApprovedVer = new ArrayList<>();
        List<DataCloudVersion> allApprovedVersions = dataCloudVersionEntityMgr.allApprovedVerions();
        for (DataCloudVersion version : allApprovedVersions) {
            if (!DataCloudVersion.Status.APPROVED.equals(version.getStatus())) {
                continue;
            }
            distinctMajorVer.add(version.getMajorVersion());
        }
        for (String majVer : distinctMajorVer) {
            String currApprVerForMajVer = dataCloudVersionService
                    .latestApprovedForMajorVersion(majVer).getVersion();
            // returning current datacloud version
            // TODO: 2.0.17 introduced new schema in AccountMasterLookup table,
            // which is not compatible with 2.0.16 versions. So removed previous
            // version from test. Add previous version back after we release
            // 2.0.18
            prevAndCurrentApprovedVer
                    .addAll(dataCloudVersionService.priorVersions(currApprVerForMajVer, 1));
        }
        Object[][] objs = new Object[prevAndCurrentApprovedVer.size() + 1][1];
        objs[0] = new Object[] { "1.0.0" };
        int i = 1;
        for (String version : prevAndCurrentApprovedVer) {
            objs[i++] = new Object[] { version };
        }
        return objs;
    }

    @DataProvider(name = "latestDataCloudVersions", parallel = true)
    public Object[][] latestDataCloudVersions() {
        List<DataCloudVersion> versions = dataCloudVersionEntityMgr.allVerions();
        Set<String> latestVersions = new HashSet<>();
        for (DataCloudVersion version : versions) {
            if (!DataCloudVersion.Status.APPROVED.equals(version.getStatus())) {
                continue;
            }
            String majorVersion = version.getMajorVersion();
            if (majorVersion.compareTo("3") < 0) {
                latestVersions.add(dataCloudVersionEntityMgr.latestApprovedForMajorVersion(majorVersion).getVersion());
            }
        }
        Object[][] objs = new Object[latestVersions.size() + 1][1];
        objs[0] = new Object[] { "1.0.0" };
        int i = 1;
        for (String latestVersion : latestVersions) {
            objs[i++] = new Object[] { latestVersion };
        }
        return objs;
    }

    private MatchInput createAvroBulkMatchInput(boolean useDir, Schema inputSchema, String dataCloudVersion,
            String avroDir, String fileName) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        matchInput.setPredefinedSelection(Predefined.RTS);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        if (useDir) {
            inputBuffer.setAvroDir(avroDir);
        } else {
            inputBuffer.setAvroDir(avroDir + "/" + fileName);
        }
        if (inputSchema != null) {
            inputBuffer.setSchema(inputSchema);
        }
        matchInput.setInputBuffer(inputBuffer);
        matchInput.setDataCloudVersion(dataCloudVersion);
        return matchInput;
    }

    private void uploadTestAVro(String avroDir, String fileName) {
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, "matchinput/SourceFile_csv.avro",
                    avroDir + "/" + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }
}
