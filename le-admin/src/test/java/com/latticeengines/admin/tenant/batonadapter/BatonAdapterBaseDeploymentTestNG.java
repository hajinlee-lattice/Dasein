package com.latticeengines.admin.tenant.batonadapter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public abstract class BatonAdapterBaseDeploymentTestNG extends AdminFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(BatonAdapterBaseDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        LatticeComponent component = getLatticeComponent();
        if (component == null) {
            throw new Exception(String.format("Component with name %s is not registered."));
        }
        deleteTenant(component);
        createTenant(component);
        bootstrap(component);
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        LatticeComponent component = getLatticeComponent();
        if (component == null) {
            log.error("No component to tear down.");
        }
        deleteTenant(component);
    }

    @Test(groups = "deployment")
    public void getDefaultConfig() throws Exception {
        LatticeComponent component = getLatticeComponent();
        if (component == null) {
            throw new Exception(String.format("Component with name %s is not registered."));
        }
        String url = String.format("%s/admin/tenants/services/%s/default", getRestHostPort(), component.getName());
        SerializableDocumentDirectory dir = restTemplate.getForObject(url, SerializableDocumentDirectory.class);
        testGetDefaultConfig(dir);
    }

    private LatticeComponent getLatticeComponent() throws Exception {
        Class<? extends LatticeComponent> componentClass = getLatticeComponentClassToTest();
        LatticeComponent comp = componentClass.newInstance();
        Map<String, LatticeComponent> componentMap = LatticeComponent.getRegisteredServices();

        return componentMap.get(comp.getName());
    }

    private void bootstrap(LatticeComponent component) throws Exception {
        String contractId = getContractId(component);
        String tenantId = getTenantId(component);

        Map<String, String> overrideProps = getOverrideProperties();
        if (overrideProps == null) {
            overrideProps = new HashMap<>();
        }
        super.bootstrap(contractId, tenantId, component.getName(), overrideProps);
    }

    private void deleteTenant(LatticeComponent component) throws Exception {
        super.deleteTenant(getContractId(component), getTenantId(component));
    }

    private void createTenant(LatticeComponent component) throws Exception {
        super.createTenant(getContractId(component), getTenantId(component));
    }

    private String getContractId(LatticeComponent component) {
        return component.getName() + "-contract";
    }

    private String getTenantId(LatticeComponent component) {
        return component.getName() + "-tenant";
    }

    public abstract Class<? extends LatticeComponent> getLatticeComponentClassToTest();

    public abstract Map<String, String> getOverrideProperties();

    public abstract void testGetDefaultConfig(SerializableDocumentDirectory dir);
}
