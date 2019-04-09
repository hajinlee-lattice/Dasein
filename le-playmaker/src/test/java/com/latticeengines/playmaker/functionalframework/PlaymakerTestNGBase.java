package com.latticeengines.playmaker.functionalframework;

import java.net.InetAddress;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.PlaymakerTenantEntityMgr;

@ContextConfiguration(locations = { "classpath:test-playmaker-context.xml" })
public class PlaymakerTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${common.test.playmaker.url}")
    protected String apiHostPort;

    @Value("${common.test.oauth.url}")
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
        tenant.setJdbcPasswordEncrypt("playmaker");
        tenant.setTenantName(getTenantName());
        return tenant;
    }

    protected String getTenantName() {
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
            // hostname can have '.' chars which will make the tenantName
            // corrupted. therefore using hash code to avoid any '.'
            hostName = "" + hostName.hashCode();
        } catch (Exception ex) {
            hostName = "localhost";
        }
        String userName = System.getProperty("user.name");
        // make sure to replace all specialChars with '_'
        userName = userName.replaceAll("[^A-Za-z0-9]", "_");

        return "LETest." + userName + "." + hostName;
    }

}
