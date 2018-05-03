package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.LdcOnlyAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportListOfEntitiesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.StartMaintenanceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.MatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.MatchListOfEntitiesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.ParallelBlockExecutionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.ResolveMetadataFromUserRefinedAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ChooseModelStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunAttributeLevelSummaryDataFlowsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.TargetMarketStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({ @Type(value = AWSBatchConfiguration.class, name = "AWSBatchConfiguration"),
        @Type(value = AWSPythonBatchConfiguration.class, name = "AWSPythonBatchConfiguration"),
        @Type(value = BaseReportStepConfiguration.class, name = "BaseReportStepConfiguration"),
        @Type(value = ChooseModelStepConfiguration.class, name = "ChooseModelStepConfiguration"),
        @Type(value = DataFlowStepConfiguration.class, name = "DataFlowStepConfiguration"),
        @Type(value = ExportToRedshiftStepConfiguration.class, name = "ExportDataToRedshiftConfiguration"),
        @Type(value = ExportToDynamoStepConfiguration.class, name = "ExportToDynamoStepConfiguration"),
        @Type(value = ExportStepConfiguration.class, name = "ExportStepConfiguration"),
        @Type(value = GenerateRatingStepConfiguration.class, name = "GenerateRatingStepConfiguration"),
        @Type(value = ImportListOfEntitiesConfiguration.class, name = "ImportListOfEntitiesConfiguration"),
        @Type(value = ImportStepConfiguration.class, name = "ImportStepConfiguration"),
        @Type(value = MatchConfiguration.class, name = "MatchConfiguration"),
        @Type(value = MatchListOfEntitiesConfiguration.class, name = "MatchListOfEntitiesConfiguration"),
        @Type(value = MatchStepConfiguration.class, name = "MatchStepConfiguration"),
        @Type(value = ModelStepConfiguration.class, name = "ModelStepConfiguration"),
        @Type(value = ParallelBlockExecutionConfiguration.class, name = "ParallelBlockExecutionConfiguration"),
        @Type(value = ProcessStepConfiguration.class, name = "ProcessStepConfiguration"),
        @Type(value = ResolveMetadataFromUserRefinedAttributesConfiguration.class, name = "ResolveMetadataFromUserRefinedAttributesConfiguration"),
        @Type(value = RTSScoreStepConfiguration.class, name = "RTSScoreStepConfiguration"),
        @Type(value = RunAttributeLevelSummaryDataFlowsConfiguration.class, name = "RunAttributeLevelSummaryDataFlowsConfiguration"),
        @Type(value = ScoreStepConfiguration.class, name = "ScoreStepConfiguration"),
        @Type(value = StartMaintenanceConfiguration.class, name = "StartMaintenanceConfiguration"),
        @Type(value = TargetMarketStepConfiguration.class, name = "TargetMarketStepConfiguration"),
        @Type(value = SetConfigurationForScoringConfiguration.class, name = "SetConfigurationForScoringConfiguration"),
        @Type(value = MatchCdlStepConfiguration.class, name = "MatchCdlStepConfiguration"),
        @Type(value = LdcOnlyAttributesConfiguration.class, name = "LdcOnlyAttributesConfiguration"),
        @Type(value = WriteOutputStepConfiguration.class, name = "WriteOutputStepConfiguration"), })
public class MicroserviceStepConfiguration extends BaseStepConfiguration {

    private String podId;

    @NotNull
    private CustomerSpace customerSpace;

    @NotEmptyString
    @NotNull
    private String microServiceHostPort;

    @JsonIgnore
    public void microserviceStepConfiguration(MicroserviceStepConfiguration config) {
        this.customerSpace = config.getCustomerSpace();
        this.microServiceHostPort = config.getMicroServiceHostPort();
    }

    @JsonProperty("customerSpace")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("microServiceHostPort")
    public String getMicroServiceHostPort() {
        return microServiceHostPort;
    }

    @JsonProperty("microServiceHostPort")
    public void setMicroServiceHostPort(String microServiceHostPort) {
        this.microServiceHostPort = microServiceHostPort;
    }

    @JsonProperty("podId")
    public String getPodId() {
        return podId;
    }

    @JsonProperty("podId")
    public void setPodId(String podId) {
        this.podId = podId;
    }

}
