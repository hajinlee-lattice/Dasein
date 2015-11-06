package com.latticeengines.workflow.exposed.build;

import com.latticeengines.common.exposed.util.JsonUtils;

public class BaseStepConfiguration {

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
