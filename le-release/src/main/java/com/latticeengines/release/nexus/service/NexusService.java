package com.latticeengines.release.nexus.service;

import org.springframework.http.ResponseEntity;

public interface NexusService {

    ResponseEntity<String> uploadArtifactToNexus(String url, String project, String version);
}
