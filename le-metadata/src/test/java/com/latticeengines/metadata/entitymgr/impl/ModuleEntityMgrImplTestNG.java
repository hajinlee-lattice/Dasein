package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.ArtifactService;
import com.latticeengines.metadata.service.ModuleService;

public class ModuleEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private ModuleService moduleService;

    @Autowired
    private ArtifactService artifactService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void getByModuleName() {
        Tenant t1 = tenantEntityMgr.findByTenantId(customerSpace1);
        MultiTenantContext.setTenant(t1);
        Module module = new Module();
        module.setName("M1");
        module.setTenant(t1);

        Artifact pmmlFile = new Artifact();
        pmmlFile.setArtifactType(ArtifactType.PMML);
        pmmlFile.setPath("/a/b/c");
        pmmlFile.setName("PMMLFile1");
        module.addArtifact(pmmlFile);

        Artifact pivotFile = new Artifact();
        pivotFile.setArtifactType(ArtifactType.PivotMapping);
        pivotFile.setPath("/d/e/f");
        pivotFile.setName("PivotFile.txt");
        module.addArtifact(pivotFile);

        artifactService.createArtifact(customerSpace1, "M1", pmmlFile.getName(), pmmlFile);
        module = moduleService.getModuleByName(customerSpace1, "M1");
        assertNotNull(module);
        assertEquals(module.getArtifacts().size(), 1);

        artifactService.createArtifact(customerSpace1, "M1", pivotFile.getName(), pivotFile);
        module = moduleService.getModuleByName(customerSpace1, "M1");
        assertNotNull(module);
        assertEquals(module.getArtifacts().size(), 2);

        Tenant t2 = tenantEntityMgr.findByTenantId(customerSpace2);
        MultiTenantContext.setTenant(t2);
        module = moduleService.getModuleByName(customerSpace2, "M1");
        assertNull(module);
    }
}
