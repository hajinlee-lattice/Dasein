package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Artifact;

public interface ArtifactInterface {

    Boolean createArtifact(String customerSpace, String moduleName, String artifactName, Artifact artifact);
    
    List<Artifact> getArtifacts(String customerSpace, String moduleName);
}
