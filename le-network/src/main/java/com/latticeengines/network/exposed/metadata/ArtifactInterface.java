package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.Artifact;

public interface ArtifactInterface {

    Boolean createArtifact(String customerSpace, String moduleName, String artifactName, Artifact artifact);

    Artifact getArtifactByPath(String customerSpace, String artifactPath);

}
