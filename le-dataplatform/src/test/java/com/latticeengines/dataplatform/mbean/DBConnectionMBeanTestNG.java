package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class DBConnectionMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Value("${dataplatform.dlorchestration.datasource.url}")
    private String dataSourceURL;

    @Value("${dataplatform.dlorchestration.datasource.user}")
    private String dataSourceUser;

    @Value("${dataplatform.dlorchestration.datasource.type}")
    private String dataSourceType;

    @Value("${db.datasource.url}")
    private String daoURL;

    @Value("${db.datasource.user}")
    private String daoUser;

    @Value("${db.datasource.type}")
    private String daoType;

    @Autowired
    private DBConnectionMBean dbcMBean;

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testDataSourceConnection() throws Exception {
        if (dataSourceType.equals("MySQL")) {
            String dsMySQLUrl = dataSourceURL + "?user=" + dataSourceUser + "&password=" + "wrongPassword";
            assertTrue(dbcMBean.getConnectionStatus(dsMySQLUrl).contains("Access denied"));
        } else if (dataSourceType.equals("SQLServer")) {
            String dsSQLServerUrl = dataSourceURL + "user=" + dataSourceUser + ";password=" + "wrongPassword";
            assertTrue(dbcMBean.getConnectionStatus(dsSQLServerUrl).contains("Login failed"));
        }
    }

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testDaoConnection() {
        if (daoType.equals("MySQL")) {
            String daoMySQLUrl = daoURL + "?user=" + daoUser + "&password=" + "wrongPassword2";
            assertTrue(dbcMBean.getConnectionStatus(daoMySQLUrl).contains("Access denied"));
        } else if (daoType.equals("SQLServer")) {
            String daoSQLServerUrl = daoURL + "user=" + daoUser + ";password=" + "wrongPassword2";
            assertTrue(dbcMBean.getConnectionStatus(daoSQLServerUrl).contains("Login failed"));
        }
    }
}
