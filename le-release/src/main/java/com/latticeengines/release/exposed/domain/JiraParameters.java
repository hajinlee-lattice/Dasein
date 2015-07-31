package com.latticeengines.release.exposed.domain;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JiraParameters {

    private JiraFields jiraFields;

    @JsonProperty("fields")
    public JiraFields getJiraFields() {
        return jiraFields;
    }

    @JsonProperty("fields")
    public void setJiraFields(JiraFields jiraFields) {
        this.jiraFields = jiraFields;
    }

    public static class JiraFields {

        public JiraFields(Map<String, String> project, String summary, Map<String, String> issueType, String backoutPlan) {
            this.project = project;
            this.summary = summary;
            this.issueType = issueType;
            this.backoutPlan = backoutPlan;
        }

        private Map<String, String> project;
        private String summary;
        private Map<String, String> issueType;
        private String backoutPlan;

        @JsonProperty("project")
        public Map<String, String> getProject() {
            return this.project;
        }

        @JsonProperty("project")
        public void setProject(Map<String, String> project) {
            this.project = project;
        }

        @JsonProperty("summary")
        public String getSummary() {
            return this.summary;
        }

        @JsonProperty("summary")
        public void setSummary(String summary) {
            this.summary = summary;
        }

        @JsonProperty("issuetype")
        public Map<String, String> getIssueType() {
            return this.issueType;
        }

        @JsonProperty("issuetype")
        public void setIssueType(Map<String, String> issueType) {
            this.issueType = issueType;
        }

        @JsonProperty("customfield_10202")
        public String getBackoutPlan() {
            return this.backoutPlan;
        }

        @JsonProperty("customfield_10202")
        public void setBackoutPlan(String backoutPlan) {
            this.backoutPlan = backoutPlan;
        }

    }
}
