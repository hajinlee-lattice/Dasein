package com.latticeengines.propdata.api.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.source.impl.AccountMaster;
import com.latticeengines.propdata.core.source.impl.AccountMasterIndex;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.util.MatchUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component
public class AccountMasterMatchResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static final String DATA_CLOUD_VERSION = "2.0.000000001";

    @Autowired
    private MatchProxy matchProxy;

    private static final String avroDir = "/tmp/AccountMasterMatchResourceDeploymentTestNG";
    private static final String avroFileName = "AccountMaster_SourceFile_csv.avro";
    private static final String podId = "AccountMasterMatchResourceDeploymentTestNG";

    @Autowired
    private AccountMaster accountMaster;

    @Autowired
    private AccountMasterIndex accountMasterIndex;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;
    @Autowired
    private MetadataProxy metadataProxy;

    @Test(groups = "deployment")
    public void testBulkMatchWithSchema() throws Exception {

        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDir);

        MatchCommand finalStatus = null;
        try {
            setupAllFiles();

            Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroDir + "/" + avroFileName));
            MatchInput input = createAvroBulkMatchInput(true, schema);
            MatchCommand command = matchProxy.matchBulk(input, podId);
            ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId);
            Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
            Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);

            finalStatus = matchProxy.bulkMatchStatus(command.getRootOperationUid());
            Assert.assertEquals(finalStatus.getApplicationId(), appId.toString());
            Assert.assertEquals(finalStatus.getRootOperationUid(), command.getRootOperationUid());
            Assert.assertEquals(finalStatus.getProgress(), 1f);
            Assert.assertEquals(finalStatus.getMatchStatus(), MatchStatus.FINISHED);
            Assert.assertEquals(finalStatus.getResultLocation(),
                    hdfsPathBuilder.constructMatchOutputDir(command.getRootOperationUid()).toString());
            // Assert.assertEquals(finalStatus.getRowsMatched(), new
            // Integer(100));

            assertOutput(finalStatus);

        } finally {
            if (finalStatus != null && HdfsUtils.fileExists(yarnConfiguration, finalStatus.getResultLocation())) {
                cleanupAvroDir(finalStatus.getResultLocation() + "/..");
            }
        }
    }

    private void assertOutput(MatchCommand finalStatus) throws Exception {
        String outputDir = finalStatus.getResultLocation();
        List<String> outputFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, outputDir + "/*.avro");
        Assert.assertEquals(outputFiles.size(), 1);
        System.out.println("Output dir=" + outputFiles.get(0));

        Schema schemaFile = AvroUtils.getSchema(yarnConfiguration, new Path(outputFiles.get(0)));
        List<Field> fields = schemaFile.getFields();
        Assert.assertEquals(fields.size(), 5);

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(outputFiles.get(0)));
        Assert.assertEquals(records.size(), 11);

        Assert.assertNotNull(records.get(0).get("EmployeesTotal").toString());

    }

    private void setupAllFiles() {
        List<Class<?>> fieldTypes = getInputAvroTypes();
        uploadDataCsv(avroDir, avroFileName, "com/latticeengines/propdata/match/AccountMasterBulkMatchInput.csv",
                fieldTypes);
        fieldTypes = getAccountMasterIndexAvroTypes();
        String dataVersion = MatchUtils.getDataVersion(DATA_CLOUD_VERSION);
        Table sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMasterIndex, dataVersion);
        uploadDataCsv(getAvroPath(sourceTable.getExtracts().get(0).getPath()), "AccountMasterIndex.avro",
                "com/latticeengines/propdata/match/AccountMasterIndex.csv", fieldTypes);

        fieldTypes = getDunsAccountMasterAvroTypes();
        sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMaster, dataVersion);
        uploadDataCsv(getAvroPath(sourceTable.getExtracts().get(0).getPath()), "AccountMaster.avro",
                "com/latticeengines/propdata/match/AccountMaster.csv", fieldTypes);
    }

    private String getAvroPath(String avroPath) {
        if (avroPath.endsWith("/*.avro") || avroPath.endsWith("/")) {
            avroPath = avroPath.substring(0, avroPath.lastIndexOf("/"));
        }
        return avroPath;
    }

    @SuppressWarnings("unchecked")
    private List<Class<?>> getInputAvroTypes() {
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.addAll(Arrays.asList(new Class<?>[] { Integer.class, String.class, String.class, String.class,
                String.class, String.class, String.class }));
        return fieldTypes;
    }

    @SuppressWarnings("unchecked")
    private List<Class<?>> getAccountMasterIndexAvroTypes() {
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.addAll(Arrays.asList(new Class<?>[] { Integer.class, String.class, String.class, String.class }));
        return fieldTypes;
    }

    @SuppressWarnings("unchecked")
    private List<Class<?>> getDunsAccountMasterAvroTypes() {
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.addAll(Arrays.asList(new Class<?>[] { Integer.class, String.class, String.class, String.class,
                String.class, String.class, String.class, String.class, String.class, String.class }));
        return fieldTypes;
    }

    private MatchInput createAvroBulkMatchInput(boolean useDir, Schema inputSchema) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.RTS);
        matchInput.setDataCloudVersion(DATA_CLOUD_VERSION);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        if (useDir) {
            inputBuffer.setAvroDir(avroDir);
        } else {
            inputBuffer.setAvroDir(avroDir + "/" + avroFileName);
        }
        if (inputSchema != null) {
            inputBuffer.setSchema(inputSchema);
        }
        matchInput.setInputBuffer(inputBuffer);
        return matchInput;
    }
}
