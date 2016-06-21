package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.metadata.entitymgr.ArtifactEntityMgr;
import com.latticeengines.metadata.service.ArtifactService;

@Component("artifactService")
public class ArtifactServiceImpl implements ArtifactService {
    
    @Autowired
    private ArtifactEntityMgr artifactEntityMgr;

    @Override
    public Artifact createArtifact(String customerSpace, String moduleName, String name, Artifact artifact) {
        Module module = new Module();
        module.setName(moduleName);
        artifact.setName(name);
        artifact.setModule(module);
        artifactEntityMgr.create(artifact);
        return artifact;
    }

    @Override
    public List<Artifact> findAll(String customerSpace, String moduleName) {
        return artifactEntityMgr.findAll();
    }

}
