package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CombineInputTableWithScoreDataFlowConfiguration.class, name = "CombineInputTableWithScoreDataFlowConfiguration"),
        @Type(value = CombineMatchDebugWithScoreDataFlowConfiguration.class, name = "CombineMatchDebugWithScoreDataFlowConfiguration"),
        @Type(value = RecalculatePercentileScoreDataFlowConfiguration.class, name = "RecalculatePercentileScoreDataFlowConfiguration"),
        @Type(value = PivotScoreAndEventConfiguration.class, name = "PivotScoreAndEventConfiguration"), })
public class BaseScoringDataFlowStepConfiguration extends DataFlowStepConfiguration {

    @Override
    public String getSwlib() {
        return SoftwareLibrary.Scoring.getName();
    }
}
