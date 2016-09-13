package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.ArtifactService;
import com.latticeengines.metadata.service.ModuleService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

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
        Tenant t1 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE1);
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

        artifactService.createArtifact(CUSTOMERSPACE1, "M1", pmmlFile.getName(), pmmlFile);
        module = moduleService.getModuleByName("M1");
        assertNotNull(module);
        assertEquals(module.getArtifacts().size(), 1);

        artifactService.createArtifact(CUSTOMERSPACE1, "M1", pivotFile.getName(), pivotFile);
        module = moduleService.getModuleByName("M1");
        assertNotNull(module);
        assertEquals(module.getArtifacts().size(), 2);
    }
}
