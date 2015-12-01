package com.latticeengines.domain.exposed.workflow;

import com.latticeengines.common.exposed.util.JsonUtils;

public class BaseStepConfiguration {

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
