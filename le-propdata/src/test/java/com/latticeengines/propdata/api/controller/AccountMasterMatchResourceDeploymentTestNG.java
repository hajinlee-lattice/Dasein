package com.latticeengines.propdata.api.controller;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.Column;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.propdata.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.source.impl.AccountMaster;
import com.latticeengines.propdata.core.source.impl.AccountMasterLookup;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component
public class AccountMasterMatchResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static final String DATA_CLOUD_VERSION = "2.0.0";

    @Autowired
    @Qualifier("matchProxyDeprecated")
    private MatchProxy matchProxy;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    private static final String avroDir = "/tmp/AccountMasterMatchResourceDeploymentTestNG";
    private static final String avroFileName = "AccountMaster_SourceFile_csv.avro";
    private static final String podId = "AccountMasterMatchResourceDeploymentTestNG";

    @Autowired
    private AccountMaster accountMaster;

    @Autowired
    private AccountMasterLookup accountMasterLookup;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Resource(name = "columnMetadataServiceDispatch")
    private ColumnMetadataService columnMetadataService;

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
        Assert.assertEquals(fields.size(), 860);

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(outputFiles.get(0)));
        Assert.assertEquals(records.size(), 14);
        Assert.assertEquals(finalStatus.getRowsMatched(), new Integer(14));
        int totalEmployees = 0;
        for (GenericRecord record : records) {
            if (record.get("EMPLOYEES_TOTAL") != null) {
                totalEmployees += totalEmployees + Integer.parseInt(record.get("EMPLOYEES_TOTAL").toString());
            }
        }
        Assert.assertTrue(totalEmployees > 0);

    }

    private void setupAllFiles() {
        List<Class<?>> fieldTypes = getInputAvroTypes();
        uploadDataCsv(avroDir, avroFileName, "com/latticeengines/propdata/match/AccountMasterBulkMatchInput.csv",
                fieldTypes, "ID__");
        fieldTypes = getAccountMasterLookupAvroTypes();
        DataCloudVersion dataVersion = dataCloudVersionEntityMgr.findVersion(DATA_CLOUD_VERSION);

        Table sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMasterLookup,
                dataVersion.getAccountLookupHdfsVersion());
        uploadDataCsv(getAvroPath(sourceTable.getExtracts().get(0).getPath()), "AccountMasterLookup.avro",
                "com/latticeengines/propdata/match/AccountMasterLookup.csv", fieldTypes, "ID__");

        fieldTypes = getDnBAccountMasterAvroTypes("com/latticeengines/propdata/match/AccountMaster.csv");
        sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMaster, dataVersion.getAccountMasterHdfsVersion());
        uploadDataCsv(getAvroPath(sourceTable.getExtracts().get(0).getPath()), "AccountMaster.avro",
                "com/latticeengines/propdata/match/AccountMaster.csv", fieldTypes, "ID__");

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
                String.class, String.class }));
        return fieldTypes;
    }

    @SuppressWarnings("unchecked")
    private List<Class<?>> getAccountMasterLookupAvroTypes() {
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.addAll(Arrays.asList(new Class<?>[] { Integer.class, String.class, String.class }));
        return fieldTypes;
    }

    private List<Class<?>> getDnBAccountMasterAvroTypes(String csvFile) {
        List<String> fieldNames = getFieldNamesFromCSVFile(csvFile);
        fieldNames = fieldNames.subList(9, fieldNames.size());
        ColumnSelection selection = getColumnSelection(fieldNames);
        List<ColumnMetadata> metadatas = columnMetadataService.fromSelection(selection, DATA_CLOUD_VERSION);
        List<Class<?>> fieldTypesInMetadata = getFieldTypesFromMetadata(metadatas);
        List<Class<?>> fieldTypes = new ArrayList<>();
        // for internal columns
        fieldTypes.add(Integer.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);

        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);

        fieldTypes.addAll(fieldTypesInMetadata);
        return fieldTypes;
    }

    private List<Class<?>> getFieldTypesFromMetadata(List<ColumnMetadata> metadatas) {
        List<Class<?>> fieldTypes = new ArrayList<>();
        for (ColumnMetadata metadata : metadatas) {
            String javaClassName = metadata.getJavaClass();
            Class<?> javaClass = mapJavaClass(javaClassName);
            fieldTypes.add(javaClass);
        }
        return fieldTypes;
    }

    private Class<?> mapJavaClass(String javaClassName) {
        javaClassName = javaClassName.toLowerCase();
        switch (javaClassName) {
        case "boolean":
            return Boolean.class;
        case "string":
            return String.class;
        case "integer":
            return Integer.class;
        case "long":
            return Long.class;
        case "double":
            return Double.class;
        }
        return String.class;
    }

    private ColumnSelection getColumnSelection(List<String> fieldNames) {
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = new ArrayList<>();
        for (String fieldName : fieldNames) {
            columns.add(new Column(fieldName));
        }
        columnSelection.setColumns(columns);
        columnSelection.setName("ColumnSelection");
        return columnSelection;
    }

    private MatchInput createAvroBulkMatchInput(boolean useDir, Schema inputSchema) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
        matchInput.setDataCloudVersion(DATA_CLOUD_VERSION);
        matchInput.setExcludePublicDomains(false);
        matchInput.setTableName("AccountMasterTest");
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
        matchInput.setReturnUnmatched(true);

        matchInput.setPredefinedSelection(Predefined.RTS);
        ColumnSelection customSelection = new ColumnSelection();
        customSelection.setName("Custom");
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("EMPLOYEES_TOTAL"));
        columns.add(new Column("SALES_VOLUME_US_DOLLARS"));
        columns.add(new Column("YEAR_STARTED"));
        customSelection.setColumns(columns);
        // matchInput.setCustomSelection(customSelection);
        return matchInput;
    }
}
