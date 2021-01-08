package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ExportTimelineSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.PublishVIDataStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish.PublishTableToElasticSearchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = PrepareMatchDataConfiguration.class, name = "PrepareMatchDataConfiguration"), //
        @JsonSubTypes.Type(value = ProcessMatchResultConfiguration.class, name = "ProcessMatchResultConfiguration"), //
        @JsonSubTypes.Type(value = CombineInputTableWithScoreDataFlowConfiguration.class, name = "CombineInputTableWithScoreDataFlowConfiguration"), //
        @JsonSubTypes.Type(value = ScoreAggregateFlowConfiguration.class, name = "ScoreAggregateFlowConfiguration"), //
        @JsonSubTypes.Type(value = PivotScoreAndEventConfiguration.class, name = "PivotScoreAndEventConfiguration"), //
        @JsonSubTypes.Type(value = CreateCdlEventTableFilterConfiguration.class, name = "CreateCdlEventTableFilterConfiguration"), //
        @JsonSubTypes.Type(value = CreateCdlEventTableConfiguration.class, name = "CreateCdlEventTableConfiguration"), //
        @JsonSubTypes.Type(value = ActivityStreamSparkStepConfiguration.class, name =
                "ActivityStreamSparkStepConfiguration"), //
        @JsonSubTypes.Type(value = TimeLineSparkStepConfiguration.class, name = "TimeLineSparkStepConfiguration"), //
        @JsonSubTypes.Type(value = ExportTimelineSparkStepConfiguration.class, name =
                "ExportTimelineSparkStepConfiguration"), //
        @JsonSubTypes.Type(value = PublishVIDataStepConfiguration.class, name =
                "PublishVIDataStepConfiguration"),
        @JsonSubTypes.Type(value = TimeLineSparkStepConfiguration.class, name = "TimeLineSparkStepConfiguration"), //
        @JsonSubTypes.Type(value = ExportToElasticSearchStepConfiguration.class, name =
                "ExportToElasticSearchStepConfiguration"), //
        @JsonSubTypes.Type(value = PublishTableToElasticSearchStepConfiguration.class, name =
                "PublishTableToElasticSearchStepConfiguration"),
        @JsonSubTypes.Type(value = EnrichWebVisitSparkStepConfiguration.class, name =
                "EnrichWebVisitSparkStepConfiguration") //
})
public class SparkJobStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer")
    private String customer;

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

}
