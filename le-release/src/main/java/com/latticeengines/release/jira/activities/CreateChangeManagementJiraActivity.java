package com.latticeengines.release.jira.activities;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.JiraParameters;
import com.latticeengines.release.exposed.domain.JiraParameters.JiraFields;
import com.latticeengines.release.exposed.domain.StatusContext;
import com.latticeengines.release.jira.service.ChangeManagementJiraService;

@Component("createChangeManagementJiraActivity")
public class CreateChangeManagementJiraActivity extends BaseActivity {

    @Autowired
    private ChangeManagementJiraService changeManagementJiraService;

    @Value("${release.jira.url}")
    private String url;

    @Autowired
    public CreateChangeManagementJiraActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public StatusContext runActivity() {
        JiraParameters jiraParameters = constructJiraParameters();
        ResponseEntity<String> response = changeManagementJiraService.createChangeManagementTicket(url, jiraParameters);
        statusContext.setStatusCode(response.getStatusCode().value());
        return statusContext;
    }

    private JiraParameters constructJiraParameters() {
        Map<String, String> project = new HashMap<>();
        project.put("key", "CR");
        String summary = String.format("LEDP Release %s on version %s", processContext.getProduct(), processContext.getReleaseVersion());
        Map<String, String> issueType = new HashMap<>();
        issueType.put("name", "Record");
        String backoutPlan = ".";
        JiraFields jiraFields = new JiraFields(project, summary, issueType, backoutPlan);
        JiraParameters jiraParameters = new JiraParameters();
        jiraParameters.setJiraFields(jiraFields);
        return jiraParameters;
    }
}
