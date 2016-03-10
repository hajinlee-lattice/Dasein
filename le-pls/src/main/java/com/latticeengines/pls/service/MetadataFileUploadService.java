package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;

public interface MetadataFileUploadService {

    List<Module> getModules();

    String uploadFile(String urlToken, String moduleName, String artifactName, InputStream inputStream);

    List<Artifact> getArtifacts(String moduleName, String urlToken);

    List<Artifact> getArtifacts(String moduleName, ArtifactType artifactType);

}
