package com.latticeengines.propdata.api.service.impl;


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.api.service.MatchCommandService;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    private static final Log log = LogFactory.getLog(MatchCommandServiceImpl.class);
    
    @Value("${propdata.matcher.host}")
    private String jdbcHost;
    @Value("${propdata.matcher.port}")
    private String jdbcPort;
    @Value("${propdata.matcher.dbname}")
    private String jdbcDb;
    @Value("${propdata.matcher.type}")
    private String jdbcType;
    @Value("${propdata.matcher.user}")
    private String jdbcUser;
    @Value("${propdata.matcher.password.encrypted}")
    private String jdbcPassword;

    @Override
    public Long createMatchCommand(String sourceTable, String destTables,
            String contractExternalID, String matchClient) {
        Long results;
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
            results = -1L;
        }
        return results;
    }

    @Override
    public String getMatchCommandStatus(String commandID, String matchClient) {
        String results;
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
        String url = "jdbc:sqlserver://" + hostURL + ":" + jdbcPort + ";databaseName=" + jdbcDb
                    + ";user=" + jdbcUser + ";password=" + jdbcPassword;
        return DriverManager.getConnection(url);
    }
}
