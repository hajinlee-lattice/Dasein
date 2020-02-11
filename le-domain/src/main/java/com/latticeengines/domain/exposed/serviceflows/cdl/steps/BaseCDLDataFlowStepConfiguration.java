package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.RedshiftPublishStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CreateCdlEventTableConfiguration.class, name = "CreateCdlEventTableConfiguration"),
        @Type(value = CreateCdlEventTableFilterConfiguration.class, name = "CreateCdlEventTableFilterConfiguration"),
        @Type(value = MatchCdlAccountConfiguration.class, name = "MatchCdlAccountConfiguration"),
        @Type(value = MatchCdlMergeConfiguration.class, name = "MatchCdlMergeConfiguration"),
        @Type(value = MatchCdlSplitConfiguration.class, name = "MatchCdlSplitConfiguration"),
        @Type(value = RedshiftPublishStepConfiguration.class, name = "RedshiftPublishStepConfiguration"),
        @Type(value = ComputeOrphanRecordsStepConfiguration.class, name = "ComputeOrphanRecordsStepConfiguration"),
})
public class BaseCDLDataFlowStepConfiguration extends DataFlowStepConfiguration {

    @Override
    public String getSwlib() {
        return SoftwareLibrary.CDL.getName();
    }

}
