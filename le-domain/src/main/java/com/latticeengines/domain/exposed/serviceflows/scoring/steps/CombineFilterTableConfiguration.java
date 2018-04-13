package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.BaseCDLDataFlowStepConfiguration;

public class CombineFilterTableConfiguration extends BaseCDLDataFlowStepConfiguration {

    public CombineFilterTableConfiguration() {
        setBeanName("combineFilterTableFlow");
        setTargetTableName(NamingUtils.uuid("CombinedFilters"));
    }
}
