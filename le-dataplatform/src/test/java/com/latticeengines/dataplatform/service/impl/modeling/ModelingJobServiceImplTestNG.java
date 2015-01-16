package com.latticeengines.dataplatform.service.impl.modeling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.runtime.load.LoadProperty;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public class ModelingJobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelingJobService modelingJobService;

    private String dataPath = "/tmp/ModelingJobServiceImplTestNG";
    private DbCreds creds = null;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("10.41.1.250") //
                .db("SP_7_Tests") //
                .port(1433) //
                .user("root") //
                .password("welcome");

        creds = new DbCreds(builder);
    }
    
    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(dataPath), true);
    }

    private Schema waitForStatusAndGetSchema(ApplicationId appId) throws Exception {
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dataPath, new HdfsFilenameFilter() {

            @Override
            public boolean accept(String filename) {
                return filename.endsWith(".avro");
            }

        });

        assertTrue(files.size() > 0);
        return AvroUtils.getSchema(yarnConfiguration, new Path(files.get(0)));
    }

    @Test(groups = { "functional" })
    public void loadDataWithDefaultConfigs() throws Exception {
        ApplicationId appId = modelingJobService.loadData("Play_11_TrainingSample_WithRevenue_2", //
                dataPath, //
                creds, //
                "Priority0.MapReduce", //
                "INTERNAL", //
                Arrays.<String> asList(new String[] { "LEAccount_ID" }), //
                new HashMap<String, String>());
        Schema schema = waitForStatusAndGetSchema(appId);
        List<Field> avroFields = schema.getFields();

        for (Field field : avroFields) {
            int sqlType = Integer.parseInt(field.getProp("sqlType"));
            
            assertTrue(sqlType != Types.TIMESTAMP && sqlType != Types.TIME, "Found timestamp or time column with name " + field.getProp("columnName"));
        }

    }

    @Test(groups = { "functional" })
    public void loadDataWithNoExcludedColumns() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(LoadProperty.EXCLUDETIMESTAMPCOLUMNS.name(), "false");
        ApplicationId appId = modelingJobService.loadData("Play_11_TrainingSample_WithRevenue_2", //
                dataPath, //
                creds, //
                "Priority0.MapReduce", //
                "INTERNAL", //
                Arrays.<String> asList(new String[] { "LEAccount_ID" }), //
                properties);
        Schema schema = waitForStatusAndGetSchema(appId);
        List<Field> avroFields = schema.getFields();

        boolean foundTimestampOrTimeCols = false;
        for (Field field : avroFields) {
            int sqlType = Integer.parseInt(field.getProp("sqlType"));
            if (sqlType == Types.TIMESTAMP || sqlType == Types.TIME) {
                foundTimestampOrTimeCols = true;
            }
        }
        assertTrue(foundTimestampOrTimeCols,
                "Timestamp or time columns should be available since the property EXCLUDETIMESTAMPCOLUMNS was set to false.");
    }
}
