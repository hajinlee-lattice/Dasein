package com.latticeengines.playmaker.functionalframework;

import java.net.InetAddress;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@ContextConfiguration(locations = { "classpath:test-playmaker-context.xml" })
public class PlaymakerTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${playmaker.test.api.hostport}")
    protected String apiHostPort;

    @Value("${playmaker.test.auth.hostport}")
    protected String authHostPort;

    protected PlaymakerTenant tenant;

    @Autowired
    protected PlaymakerTenantEntityMgr playMakerEntityMgr;

    public void beforeClass() {
        tenant = getTenant();

        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
            System.out.println("Warning=" + ex.getMessage());
        }
    }

    public PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.118;instanceName=SQL2012STD;databaseName=PlayMakerDB");
        tenant.setJdbcUserName("playmaker");
        tenant.setJdbcPassword("playmaker");
        tenant.setTenantName(getTenantName());
        return tenant;
    }


    protected String getTenantName() {
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            hostName = "localhost";
        }
        return "test." + System.getProperty("user.name") + "." + hostName;
    }

}
