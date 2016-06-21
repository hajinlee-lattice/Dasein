package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.ArtifactEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.ArtifactService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ArtifactEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    
    @Autowired
    private ArtifactService artifactService;
    
    @Autowired
    private ArtifactEntityMgr artifactEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void create() {
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
        artifactService.createArtifact(CUSTOMERSPACE1, "M1", pivotFile.getName(), pivotFile);

        List<Artifact> artifacts = artifactEntityMgr.findAll();
        assertEquals(artifacts.size(), 2);

        Tenant t2 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE2);
        MultiTenantContext.setTenant(t2);
        artifacts = artifactEntityMgr.findAll();
        assertEquals(artifacts.size(), 0);
    }
}
