package com.latticeengines.domain.exposed.cdl.tray;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TrayConnectorTestMetadata {

    @JsonProperty("testName")
    private String testName;

    @JsonProperty("description")
    private String description;

    @JsonProperty("trigger")
    private TriggerMetadata trigger;

    @JsonProperty("validation")
    private ValidationMetadata validation;

    public String getTestName() {
        return this.testName;
    }

    public void setTestName(String testName) {
        this.testName = testName;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public TriggerMetadata getTrigger() {
        return this.trigger;
    }

    public void setTrigger(TriggerMetadata trigger) {
        this.trigger = trigger;
    }

    public ValidationMetadata getValidation() {
        return this.validation;
    }

    public void setValidation(ValidationMetadata validation) {
        this.validation = validation;
    }

    public static class TriggerMetadata {

        @JsonProperty("config")
        private TriggerConfig config;

        @JsonProperty("message")
        private ExternalIntegrationMessageBody message;

        public TriggerConfig getTriggerConfig() {
            return this.config;
        }

        public void setTriggerConfig(TriggerConfig config) {
            this.config = config;
        }

        public ExternalIntegrationMessageBody getMessage() {
            return this.message;
        }

        public void setMessage(ExternalIntegrationMessageBody message) {
            this.message = message;
        }

    }

    public static class TriggerConfig {

        @JsonProperty("generateSolutionInstance")
        private boolean generateSolutionInstance;

        @JsonProperty("generateExternalAudience")
        private boolean generateExternalAudience;

        public boolean getGenerateSolutionInstance() {
            return this.generateSolutionInstance;
        }

        public void setGenerateSolutionInstance(boolean generateSolutionInstance) {
            this.generateSolutionInstance = generateSolutionInstance;
        }

        public boolean getGenerateExternalAudience() {
            return this.generateExternalAudience;
        }

        public void setGenerateExternalAudience(boolean generateExternalAudience) {
            this.generateExternalAudience = generateExternalAudience;
        }

    }

    private static class ValidationMetadata {

        @JsonProperty("config")
        private ValidationConfig config;

        @JsonProperty("messages")
        private List<DataIntegrationStatusMonitorMessage> messages;

        public ValidationConfig getValidationConfig() {
            return this.config;
        }

        public void setValidationConfig(ValidationConfig config) {
            this.config = config;
        }

        public List<DataIntegrationStatusMonitorMessage> getMessages() {
            return this.messages;
        }

        public void setMessages(List<DataIntegrationStatusMonitorMessage> messages) {
            this.messages = messages;
        }

    }

    private static class ValidationConfig {

        @JsonProperty("deleteSolutionInstance")
        private boolean deleteSolutionInstance;

        @JsonProperty("deleteExternalAudienc")
        private boolean deleteExternalAudience;

        public boolean getDeleteSolutionInstance() {
            return this.deleteSolutionInstance;
        }

        public void setDeleteSolutionInstance(boolean deleteSolutionInstance) {
            this.deleteSolutionInstance = deleteSolutionInstance;
        }

        public boolean getGenerateExternalAudience() {
            return this.deleteExternalAudience;
        }

        public void setDeleteExternalAudience(boolean deleteExternalAudience) {
            this.deleteExternalAudience = deleteExternalAudience;
        }
    }

}
