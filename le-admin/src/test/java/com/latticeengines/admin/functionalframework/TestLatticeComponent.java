package com.latticeengines.admin.functionalframework;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

@Component
public class TestLatticeComponent extends LatticeComponent {

    private final LatticeComponentInstaller installer = new TestLatticeComponentInstaller();
    private final CustomerSpaceServiceUpgrader upgrader = new TestLatticeComponentUpgrader();
    public static final String componentName = "TestComponent";

    private final BatonService batonService = new BatonServiceImpl();
    private CustomerSpaceServiceScope scope = null;

    public TestLatticeComponent() {
        //song: override properties are stored at the /Spaces/{spaceId} node.
        Map<String, String> overrideProps = new HashMap<>();
        overrideProps.put("PROP1", "abc");
        scope = new CustomerSpaceServiceScope("CONTRACT1", //
                "TENANT1", //
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, //
                getName(), //
                overrideProps);
    }

    public CustomerSpaceServiceScope getScope() {
        return scope;
    }

    public void tearDown() throws Exception {
        batonService.discardService(this.getName());
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "testcomponent_default.json";
        String metadataJson = "testcomponent_metadata.json";

        String podId = CamilleEnvironment.getPodId();

        Path defaultRootPath = PathBuilder.buildServiceDefaultConfigPath(podId, this.getName());
        // deserialize and upload configuration json
        DocumentDirectory dir = LatticeComponent.constructConfigDirectory(defaultJson, metadataJson);
        batonService.loadDirectory(dir, defaultRootPath);

        // deserialize and upload metadata json
        Path metadataRootPath = PathBuilder.buildServiceConfigSchemaPath(podId, this.getName());
        dir = LatticeComponent.constructMetadataDirectory(defaultJson, metadataJson);
        batonService.loadDirectory(dir, metadataRootPath);

        Camille camille = CamilleEnvironment.getCamille();
        try {
            return camille.exists(defaultRootPath) && camille.exists(metadataRootPath);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getName() { return componentName; }

    @Override
    public void setName(String name) { }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() { return installer; }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() { return upgrader; }

    @Override
    public String getVersionString() { return "1.0"; }

}
