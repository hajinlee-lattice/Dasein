package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
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

    protected boolean doYarnClusterSetup() {
        return false;
    }

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
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(TEMP_EVENTTABLE, true);
        modelCommandEntityMgr.create(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        debugProcessorImpl.execute(command, commandParameters);

        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        String tableCopy = "";
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            // select name from sys.tables where [name] = 'X'
            tableCopy = dlOrchestrationJdbcTemplate.queryForObject("select [name] from sys.tables where [name] = '"
                    + TEMP_EVENTTABLE_COPY + "'", String.class);
        } else {
            // MySQL Connector Java
            tableCopy = dlOrchestrationJdbcTemplate.queryForObject("SHOW TABLES LIKE '" + TEMP_EVENTTABLE_COPY + "'",
                    String.class);
        }
        assertEquals(tableCopy, TEMP_EVENTTABLE_COPY);
    }
}
