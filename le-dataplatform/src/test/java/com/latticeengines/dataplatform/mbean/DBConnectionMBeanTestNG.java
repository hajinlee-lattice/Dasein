package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import static org.testng.Assert.assertTrue;


public class DBConnectionMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Value("${dataplatform.dlorchestration.datasource.url}")
    private String dataSourceURL;

    @Value("${dataplatform.dlorchestration.datasource.user}")
    private String dataSourceUser;

    @Value("${dataplatform.dlorchestration.datasource.type}")
    private String dataSourceType;

    @Value("${dataplatform.dao.datasource.url}")
    private String daoURL;

    @Value("${dataplatform.dao.datasource.user}")
    private String daoUser;

    @Value("${dataplatform.dao.datasource.type}")
    private String daoType;

    private DBConnectionMBean dbc;

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        dbc = new DBConnectionMBean();
    }

    @Test(groups = "functional", enabled = true)
    public void testDataSourceConnectionStatus() throws Exception {
        if (dataSourceType.equals("MySQL")) {
            String dsMySQLUrl = dataSourceURL + "?user=" + dataSourceUser + "&password=" + "wrongPassword";
            assertTrue(dbc.getConnectionStatus(dsMySQLUrl).contains("Access denied"));
        } else if (dataSourceType.equals("SQLServer")) {
            String dsSQLServerUrl = dataSourceURL + "user=" + dataSourceUser + ";password=" + "wrongPassword";
            assertTrue(dbc.getConnectionStatus(dsSQLServerUrl).contains("Login failed"));
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testDaoConnectionStatus() {
        if (daoType.equals("MySQL")) {
            String daoMySQLUrl = daoURL + "?user=" + daoUser + "&password=" + "wrongPassword2";
            assertTrue(dbc.getConnectionStatus(daoMySQLUrl).contains("Access denied"));
        } else if (daoType.equals("SQLServer")) {
            String daoSQLServerUrl = daoURL + "user=" + daoUser + ";password=" + "wrongPassword2";
            assertTrue(dbc.getConnectionStatus(daoSQLServerUrl).contains("Login failed"));
        }
    }
}
