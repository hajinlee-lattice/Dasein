package com.latticeengines.propdata.api.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;

import com.latticeengines.propdata.api.service.MatchCommandService;

@Component("MatchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    private static final Log log = LogFactory.getLog(MatchCommandServiceImpl.class);
    
    @Value("${propdata.matchcommand.host}")
    private String jdbcHost;
    @Value("${propdata.matchcommand.port}")
    private String jdbcPort;
    @Value("${propdata.matchcommand.dbname}")
    private String jdbcDb;
    @Value("${propdata.matchcommand.type}")
    private String jdbcType;
    @Value("${propdata.matchcommand.user}")
    private String jdbcUser;
    @Value("${propdata.matchcommand.password.encrypted}")
    private String jdbcPassword;

    @Override
    public Long createMatchCommand(String sourceTable, String destTables,
            String contractExternalID, String matchClient) {
        Long results = new Long(-1);
        if(matchClient==null)
            matchClient=jdbcHost;
        
        try {
            Connection conn = getConnection(matchClient);
            CallableStatement cstmt = conn.prepareCall("{call dbo.MatcherClient_CreateCommand(?, ?, ?, ?)}");
            cstmt.setString("inputSourceTableName", sourceTable);
            cstmt.setString("contractExternalID", contractExternalID);
            cstmt.setString("destTables", destTables);
            cstmt.registerOutParameter("commandID", java.sql.Types.INTEGER);
            cstmt.execute();
            results = cstmt.getLong("commandID");
            cstmt.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
            results = new Long(-1);
        }
        return results;
    }

    @Override
    public String getMatchCommandStatus(String commandID, String matchClient) {
        String results = "";
        if(matchClient==null)
            matchClient=jdbcHost;
        
        try {
            Connection conn = getConnection(matchClient);
            CallableStatement cstmt = conn.prepareCall("{call dbo.MatcherClient_GetCommandStatus(?, ?)}");
            cstmt.setInt("commandID",Integer.valueOf(commandID));
            cstmt.registerOutParameter("status", java.sql.Types.NVARCHAR);
            cstmt.execute();
            results = cstmt.getString("status");
            cstmt.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
            results = "-1";
        }
        return results;
    }
    
    private Connection getConnection(String hostURL) throws SQLException{
        String url = "";
        url = "jdbc:sqlserver://" + hostURL + ":" + jdbcPort + ";databaseName=" + jdbcDb 
                    + ";user=" + jdbcUser + ";password=" + jdbcPassword;
        /*url = "jdbc:sqlserver://192.168.4.44:" + jdbcPort + ";databaseName=" + jdbcDb 
                + ";integratedSecurity=true;user=" + jdbcUser + ";password=" + jdbcPassword;*/
            
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }
}
