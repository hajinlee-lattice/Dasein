package com.latticeengines.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.lifecycle.PodLifecycleManager;
import com.latticeengines.camille.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.properties.PropertiesManager;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class PropertiesManagerUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testPodScope() throws Exception {
        PodScope scope = new PodScope();
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        Path path = new Path("/foo");

        PropertiesManager<ConfigurationScope> pm = new PropertiesManager<ConfigurationScope>(scope, path);

        double d = 10;
        String dblName = "myDouble";
        int i = 2 ^ 8;
        String intName = "myInt";
        String s = new java.util.Date().toString();
        String strName = "myString";
        pm.setDoubleProperty(dblName, d);
        pm.setIntProperty(intName, i);
        pm.setStringProperty(strName, s);

        Assert.assertEquals(pm.getStringProperty(strName), s);
        Assert.assertEquals(pm.getDoubleProperty(dblName), d);
        Assert.assertEquals(pm.getIntProperty(intName), i);
    }

    @Test(groups = "unit")
    public void testContractScope() throws Exception {
        ContractScope scope = new ContractScope("MyContract");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        Path path = new Path("/foo");

        PropertiesManager<ConfigurationScope> pm = new PropertiesManager<ConfigurationScope>(scope, path);

        double d = 10;
        String dblName = "myDouble";
        int i = 2 ^ 8;
        String intName = "myInt";
        String s = new java.util.Date().toString();
        String strName = "myString";
        pm.setDoubleProperty(dblName, d);
        pm.setIntProperty(intName, i);
        pm.setStringProperty(strName, s);

        Assert.assertEquals(pm.getStringProperty(strName), s);
        Assert.assertEquals(pm.getDoubleProperty(dblName), d);
        Assert.assertEquals(pm.getIntProperty(intName), i);
    }

    @Test(groups = "unit")
    public void testTenantScope() throws Exception {
        TenantScope scope = new TenantScope("MyContract", "MyTenant");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        TenantLifecycleManager.create(scope.getContractID(), scope.getTenantID(), "MySpace");
        Path path = new Path("/foo");

        PropertiesManager<ConfigurationScope> pm = new PropertiesManager<ConfigurationScope>(scope, path);

        double d = 10;
        String dblName = "myDouble";
        int i = 2 ^ 8;
        String intName = "myInt";
        String s = new java.util.Date().toString();
        String strName = "myString";
        pm.setDoubleProperty(dblName, d);
        pm.setIntProperty(intName, i);
        pm.setStringProperty(strName, s);

        Assert.assertEquals(pm.getStringProperty(strName), s);
        Assert.assertEquals(pm.getDoubleProperty(dblName), d);
        Assert.assertEquals(pm.getIntProperty(intName), i);
    }

    @Test(groups = "unit")
    public void testSpaceScope() throws Exception {
        CustomerSpaceScope scope = new CustomerSpaceScope("MyContract", "MyTenant", "MySpace");
        PodLifecycleManager.create(CamilleEnvironment.getPodId());
        ContractLifecycleManager.create(scope.getContractID());
        TenantLifecycleManager.create(scope.getContractID(), scope.getTenantID(), "DefaultSpace");
        SpaceLifecycleManager.create(scope.getContractID(), scope.getTenantID(), scope.getSpaceID());
        Path path = new Path("/foo");

        PropertiesManager<ConfigurationScope> pm = new PropertiesManager<ConfigurationScope>(scope, path);

        double d = 10;
        String dblName = "myDouble";
        int i = 2 ^ 8;
        String intName = "myInt";
        String s = new java.util.Date().toString();
        String strName = "myString";
        pm.setDoubleProperty(dblName, d);
        pm.setIntProperty(intName, i);
        pm.setStringProperty(strName, s);

        Assert.assertEquals(pm.getStringProperty(strName), s);
        Assert.assertEquals(pm.getDoubleProperty(dblName), d);
        Assert.assertEquals(pm.getIntProperty(intName), i);
    }
}
