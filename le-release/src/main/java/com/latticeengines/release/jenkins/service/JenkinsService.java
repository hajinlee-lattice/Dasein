package com.latticeengines.release.jenkins.service;

import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.release.exposed.domain.JenkinsParameters;

public interface JenkinsService {

    ResponseEntity<String> triggerJenkinsJobWithOutParameters(String url);

    ResponseEntity<String> triggerJenkinsJobWithParameters(String url, JenkinsParameters jenkinsParameters);

    JsonNode getLastBuildStatus(String url);

    ResponseEntity<String> updateSVNBranchName(String url, String version);

    String getConfiguration(String url);
}
