package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Artifact;

public interface ArtifactService {

    Artifact createArtifact(String customerSpace, String moduleName, String name, Artifact artifact);

    List<Artifact> findAll(String customerSpace, String moduleName);

    Artifact getArtifactByPath(String customerSpace, String artifactPath);
}
