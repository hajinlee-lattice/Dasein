package com.latticeengines.release.jira.service;

import org.springframework.http.ResponseEntity;

import com.latticeengines.release.exposed.domain.JiraParameters;

public interface ChangeManagementJiraService{

    ResponseEntity<String> createChangeManagementTicket(String url, JiraParameters jiraParameters);
}