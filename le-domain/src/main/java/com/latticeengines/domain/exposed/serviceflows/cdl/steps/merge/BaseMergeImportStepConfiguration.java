package com.latticeengines.domain.exposed.serviceflows.cdl.steps.merge;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public abstract class BaseMergeImportStepConfiguration extends BaseWrapperStepConfiguration {

    public abstract BusinessEntity getMainEntity();

}
