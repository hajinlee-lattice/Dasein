package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByDateRangeActionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CalculateDeltaStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.QueuePlayLaunchesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateIntentAlertArtifactsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateTimelineExportUniverseStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MockActivityStoreConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SendIntentAlertEmailStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.CopyListSegmentCSVConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExportListSegmentCSVToS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExtractListSegmentCSVConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.AggregateBrokerFileConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.BrokerDataFullLoadConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportVdbTableStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkScriptStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.IngestionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PublishConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.TransformationStepExecutionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkMatchInputConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PublishEntityMatchStagingConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({ //
        @Type(value = BaseWrapperStepConfiguration.class, name = "BaseWrapperStepConfiguration"), //
        @Type(value = CombineStatisticsConfiguration.class, name = "CombineStatisticsConfiguration"), //
        @Type(value = ImportVdbTableStepConfiguration.class, name = "ImportVdbTableStepConfiguration"), //
        @Type(value = IngestionStepConfiguration.class, name = "IngestionStepConfiguration"), //
        @Type(value = MicroserviceStepConfiguration.class, name = "MicroserviceStepConfiguration"), //
        @Type(value = PrepareTransformationStepInputConfiguration.class, name = "PrepareTransformationStepInputConfiguration"), //
        @Type(value = PlayLaunchInitStepConfiguration.class, name = "PlayLaunchInitStepConfiguration"), //
        @Type(value = PrepareBulkMatchInputConfiguration.class, name = "PrepareBulkMatchInputConfiguration"), //
        @Type(value = PublishConfiguration.class, name = "PublishConfiguration"), //
        @Type(value = SegmentExportStepConfiguration.class, name = "SegmentExportStepConfiguration"), //
        @Type(value = TransformationStepExecutionConfiguration.class, name = "TransformationStepExecutionConfiguration"), //
        @Type(value = CommitEntityMatchConfiguration.class, name = "CommitEntityMatchConfiguration"), //
        @Type(value = SparkJobStepConfiguration.class, name = "SparkJobStepConfiguration"), //
        @Type(value = SparkScriptStepConfiguration.class, name = "SparkScriptStepConfiguration"), //
        @Type(value = EntityExportStepConfiguration.class, name = "EntityExportStepConfiguration"), //
        @Type(value = QueuePlayLaunchesStepConfiguration.class, name = "QueuePlayLaunchesStepConfiguration"), //
        @Type(value = CalculateDeltaStepConfiguration.class, name = "CalculateDeltaStepConfiguration"), //
        @Type(value = MockActivityStoreConfiguration.class, name = "MockActivityStoreConfiguration"), //
        @Type(value = PublishEntityMatchStagingConfiguration.class, name = "PublishEntityMatchStagingConfiguration"), //
        @Type(value = LegacyDeleteByDateRangeActionConfiguration.class, name = "LegacyDeleteByDateRangeActionConfiguration"), //
        @Type(value = GenerateTimelineExportUniverseStepConfiguration.class, name = "GenerateTimelineUniverseStepConfiguration"), //
        @Type(value = GenerateIntentAlertArtifactsStepConfiguration.class, name = "GenerateIntentAlertArtifactsStepConfiguration"), //
        @Type(value = SendIntentAlertEmailStepConfiguration.class, name = "SendIntentAlertEmailStepConfiguration"), //
        @Type(value = ExtractListSegmentCSVConfiguration.class, name = "ExtractListSegmentCSVConfiguration"), //
        @Type(value = CopyListSegmentCSVConfiguration.class, name = "CopyListSegmentCSVConfiguration"), //
        @Type(value = ExportListSegmentCSVToS3Configuration.class, name = "ExportListSegmentCSVToS3Configuration"),
        @Type(value = BaseMultiTransformationStepConfiguration.class, name = "BaseMultiTransformationStepConfiguration"), //
        @Type(value = BrokerDataFullLoadConfiguration.class, name = "InitialLoadBrokerDataConfiguration"), //
        @Type(value = AggregateBrokerFileConfiguration.class, name = "AggregateBrokerFileConfiguration")})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseStepConfiguration {

    @JsonProperty("internal_resource_host_port")
    private String internalResourceHostPort;

    @JsonProperty("skip_step")
    private boolean skipStep = false;

    @JsonProperty("name")
    private String name = this.getClass().getSimpleName();

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    public boolean isSkipStep() {
        return skipStep;
    }

    public void setSkipStep(boolean skipStep) {
        this.skipStep = skipStep;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
