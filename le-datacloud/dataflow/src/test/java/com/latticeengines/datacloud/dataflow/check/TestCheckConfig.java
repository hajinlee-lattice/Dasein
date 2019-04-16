package com.latticeengines.datacloud.dataflow.check;

import com.latticeengines.domain.exposed.datacloud.check.CheckParam;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class TestCheckConfig extends TransformerConfig {

    private CheckParam checkParam;

    @SuppressWarnings("unused")
    private TestCheckConfig(){}

    TestCheckConfig(CheckParam checkParam) {
        this.checkParam = checkParam;
    }

    public CheckParam getCheckParam() {
        return checkParam;
    }

    public void setCheckParam(CheckParam checkParam) {
        this.checkParam = checkParam;
    }
}
