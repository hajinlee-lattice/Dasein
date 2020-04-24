package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProfileAccountActivityMetricsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProfileContactActivityMetricsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;

public class ProcessActivityStreamWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessActivityStreamWorkflowConfiguration configuration = new ProcessActivityStreamWorkflowConfiguration();
        private ProcessActivityStreamStepConfiguration processStepConfiguration = new ProcessActivityStreamStepConfiguration();
        private ActivityStreamSparkStepConfiguration activityStreamSparkConfiguration = new ActivityStreamSparkStepConfiguration();
        private ProfileAccountActivityMetricsStepConfiguration profileAccountActivityMetricsStepConfiguration = new ProfileAccountActivityMetricsStepConfiguration();
        private ProfileContactActivityMetricsStepConfiguration profileContactActivityMetricsStepConfiguration = new ProfileContactActivityMetricsStepConfiguration();
        private TimeLineSparkStepConfiguration timeLineSparkStepConfiguration = new TimeLineSparkStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            activityStreamSparkConfiguration.setCustomer(customerSpace.toString());
            profileAccountActivityMetricsStepConfiguration.setCustomerSpace(customerSpace);
            profileContactActivityMetricsStepConfiguration.setCustomerSpace(customerSpace);
            timeLineSparkStepConfiguration.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            activityStreamSparkConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            profileAccountActivityMetricsStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            profileContactActivityMetricsStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            timeLineSparkStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processStepConfiguration.setSkipStep(!entityMatchEnabled);
            activityStreamSparkConfiguration.setSkipStep(!entityMatchEnabled);
            processStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            activityStreamSparkConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            profileAccountActivityMetricsStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            profileContactActivityMetricsStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder entityMatchGAOnly(boolean entityMatchGAOnly) {
            processStepConfiguration.setEntityMatchGAOnly(entityMatchGAOnly);
            activityStreamSparkConfiguration.setEntityMatchGAOnly(entityMatchGAOnly);
            return this;
        }

        public Builder activeRawStreamTables(Map<String, String> rawStreamTables) {
            processStepConfiguration.setActiveRawStreamTables(rawStreamTables);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> activityStreams) {
            processStepConfiguration.setActivityStreamMap(activityStreams);
            activityStreamSparkConfiguration.setActivityStreamMap(activityStreams);
            timeLineSparkStepConfiguration.setActivityStreamMap(activityStreams);
            return this;
        }

        public Builder activityMetricsGroups(Map<String, ActivityMetricsGroup> groups) {
            processStepConfiguration.setActivityMetricsGroupMap(groups);
            activityStreamSparkConfiguration.setActivityMetricsGroupMap(groups);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            processStepConfiguration.setStreamImports(activityStreamImports);
            return this;
        }

        public Builder setReplaceMode(boolean isReplaceMode) {
            processStepConfiguration.setReplaceMode(isReplaceMode);
            return this;
        }

        public Builder setRematchMode(boolean isRematchMode) {
            processStepConfiguration.setRematchMode(isRematchMode);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities) && entities.contains(BusinessEntity.ActivityStream)) {
                activityStreamSparkConfiguration.setShouldRebuild(true);
                timeLineSparkStepConfiguration.setShouldRebuild(true);
            }
            return this;
        }

        public Builder activeTimelineList(List<TimeLine> timeLineList) {
            timeLineSparkStepConfiguration.setTimeLineList(timeLineList);
            return this;
        }

        public Builder templateToSystemTypeMap(Map<String, S3ImportSystem.SystemType> templateToSystemTypeMap) {
            timeLineSparkStepConfiguration.setTemplateToSystemTypeMap(templateToSystemTypeMap);
            return this;
        }

        public ProcessActivityStreamWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processActivityStreamWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processStepConfiguration);
            configuration.add(activityStreamSparkConfiguration);
            configuration.add(profileAccountActivityMetricsStepConfiguration);
            configuration.add(profileContactActivityMetricsStepConfiguration);
            configuration.add(timeLineSparkStepConfiguration);
            return configuration;
        }
    }
}
