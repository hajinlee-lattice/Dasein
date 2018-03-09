package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTargetTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlMergeConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlSplitConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.RedshiftPublishStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({ @Type(value = ProcessMatchResultConfiguration.class, name = "ProcessMatchResultConfiguration") })
public class BaseCoreDataFlowStepConfiguration extends DataFlowStepConfiguration {

    @Override
    public String getSwlib() {
        return SoftwareLibrary.LeadPrioritization.getName();
    }

}
