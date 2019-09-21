package com.latticeengines.dataplatform.mbean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("dbcMBean")
@ManagedResource(objectName = "Diagnostics:name=DBConnectionCheck")
public class DBConnectionMBean {

    @Deprecated
    @Value("${dataplatform.dlorchestration.datasource.url}")
    private String dataSourceURL;

    @Deprecated
    @Value("${dataplatform.dlorchestration.datasource.user}")
    private String dataSourceUser;

    @Deprecated
    @Value("${dataplatform.dlorchestration.datasource.password.encrypted}")
    private String dataSourcePasswd;

    @Deprecated
    @Value("${dataplatform.dlorchestration.datasource.type}")
    private String dataSourceType;

    @Value("${db.datasource.url}")
    private String daoURL;

    @Value("${db.datasource.user}")
    private String daoUser;

    @Value("${db.datasource.password.encrypted}")
    private String daoPasswd;

    @Value("${db.datasource.type}")
    private String daoType;

    @ManagedOperation(description = "Check ledp Connection ")
    public String checkLedpConnection() {
        String url = "";
        if (daoType.equals("MySQL")) {
            url = constructMySQLURL(daoURL, daoUser, daoPasswd);
        } else if (daoType.equals("SQLServer")) {
            url = constructSQLServerURL(daoURL, daoUser, daoPasswd);
        }
        return getConnectionStatus(url);
    }

    @Deprecated
    @ManagedOperation(description = "Check LeadScoringDB Connection")
    public String checkLeadScoringDBConnection() {
        String url = "";
        if (dataSourceType.equals("MySQL")) {
            url = constructMySQLURL(dataSourceURL, dataSourceUser, dataSourcePasswd);
        } else if (dataSourceType.equals("SQLServer")) {
            url = constructSQLServerURL(dataSourceURL, dataSourceUser, dataSourcePasswd);
        }
        return getConnectionStatus(url);
    }

    public String constructMySQLURL(String url, String user, String passwd) {
        return url + "?user=" + user + "&password=" + passwd;
    }

    public String constructSQLServerURL(String url, String user, String passwd) {
        return url + "user=" + user + ";password=" + passwd;
    }

    public String getConnectionStatus(String url) {
        try {
            @SuppressWarnings("unused")
            Connection conn = DriverManager.getConnection(url);
            return "Successfully connected to " + url.substring(0, url.indexOf("password") - 1) + "\n";
        } catch (SQLException sqle) {
            return "Failed to connect to " + url.substring(0, url.indexOf("password") - 1) + ".\n" + sqle.getMessage();
        }
    }
}
