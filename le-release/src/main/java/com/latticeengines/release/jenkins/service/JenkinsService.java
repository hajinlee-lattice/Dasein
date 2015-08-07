package com.latticeengines.release.jenkins.service;

import org.springframework.http.ResponseEntity;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.JenkinsParameters;

public interface JenkinsService {

    ResponseEntity<String> triggerJenkinsJobWithOutParameters(String url);

    ResponseEntity<String> triggerJenkinsJobWithParameters(String url, JenkinsParameters jenkinsParameters);

    JenkinsBuildStatus getLastBuildStatus(String url);

    ResponseEntity<String> updateSVNBranchName(String url, String version);

    String getConfiguration(String url);
}
