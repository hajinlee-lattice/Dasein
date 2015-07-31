package com.latticeengines.release.jenkins.service;

import org.springframework.http.ResponseEntity;

public interface JenkinsService {

    ResponseEntity<String> triggerJenkinsJob();
}
