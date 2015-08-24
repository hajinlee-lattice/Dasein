package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

public class DebugProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final String TEMP_EVENTTABLE = "DebugProcessorImplTestNG_eventtable";
    private static final String TEMP_EVENTTABLE_COPY = TEMP_EVENTTABLE + DebugProcessorImpl.COPY_SUFFIX;

    @Autowired
    private DebugProcessorImpl debugProcessorImpl;

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @BeforeClass(groups = "functional")
    public void beforeClass() throws Exception {
        initMocks(this);

        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + TEMP_EVENTTABLE + "', 'U') IS NOT NULL DROP TABLE "
                    + TEMP_EVENTTABLE);
        } else {
            // MySQL Connector Java
            dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE);
        }
        dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE + " (Id int)");
    }

    @AfterClass(groups = { "functional" })
    public void cleanup() {
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE_COPY);
        super.clearTables();
    }

    @Test(groups = "functional")
    public void testExecute() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L,TEMP_EVENTTABLE, true,
                false);
        modelCommandEntityMgr.create(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        debugProcessorImpl.execute(command, commandParameters);

        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        String tableCopy = "";
        Object dataSize = null;
        Object rowSize = null;
        int columnSize = 0;
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            // select name from sys.tables where [name] = 'X'
            tableCopy = dlOrchestrationJdbcTemplate.queryForObject("select [name] from sys.tables where [name] = '"
                    + TEMP_EVENTTABLE_COPY + "'", String.class);
            Map<String, Object> resMap = dlOrchestrationJdbcTemplate.queryForMap("EXEC sp_spaceused N'"
                    + TEMP_EVENTTABLE + "'");
            dataSize = (String) resMap.get("data");
            rowSize = (String) resMap.get("rows");
            columnSize = dlOrchestrationJdbcTemplate
                    .queryForObject(
                            "SELECT COUNT(*) FROM sys.columns where object_id = OBJECT_ID('[" + command.getEventTable()
                                    + "]')", Integer.class);
        } else {
            // MySQL Connector Java
            tableCopy = dlOrchestrationJdbcTemplate.queryForObject("SHOW TABLES LIKE '" + TEMP_EVENTTABLE_COPY + "'",
                    String.class);
            Map<String, Object> resMap = dlOrchestrationJdbcTemplate.queryForMap("show table status where name = '"
                    + command.getEventTable() + "'");
            dataSize = (BigInteger) resMap.get("Data_length");

            rowSize = (BigInteger) resMap.get("Rows");

            columnSize = dlOrchestrationJdbcTemplate.queryForObject(
                    "select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name='" + command.getEventTable()
                            + "'", Integer.class);
        }
        assertNotNull(dataSize);
        assertNotNull(rowSize);
        assertTrue(columnSize > 0);
        assertEquals(tableCopy, TEMP_EVENTTABLE_COPY);
    }
}
