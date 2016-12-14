package com.latticeengines.admin.tenant.batonadapter.modeling;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.encryption.exposed.service.DataEncryptionService;

@Component
public class ModelingComponent extends LatticeComponent {
    public static final String componentName = "Modeling";

    @Value("${admin.modeling.dryrun}")
    private boolean dryrun;

    @Autowired
    private DataEncryptionService dataEncryptionService;

    @Autowired
    private ModelingComponentManager modelingComponentManager;

    private ModelingInstaller installer = new ModelingInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new ModelingUpgrader();
    private ModelingDestroyer destroyer = new ModelingDestroyer();

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Arrays.asList(LatticeProduct.LPA, LatticeProduct.LPA3, LatticeProduct.PD, LatticeProduct.CG));
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(dryrun);
        installer.setDataEncryptionService(dataEncryptionService);

        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public CustomerSpaceServiceDestroyer getDestroyer() {
        destroyer.setDataEncryptionService(dataEncryptionService);
        destroyer.setModelingComponentManager(modelingComponentManager);
        return destroyer;
    }

    @Override
    public String getVersionString() {
        return "1.0";
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "modeling_default.json";
        String metadataJson = "modeling_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        return true;
    }
}
