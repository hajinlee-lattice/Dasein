package com.latticeengines.domain.exposed.datacloud.transformation.config.atlas;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class ConsolidateRetainFieldConfig extends TransformerConfig {

    private List<String> fieldsToRetain;

    public List<String> getFieldsToRetain() {
        return fieldsToRetain;
    }

    public void setFieldsToRetain(List<String> fieldsToRetain) {
        this.fieldsToRetain = fieldsToRetain;
    }

}
