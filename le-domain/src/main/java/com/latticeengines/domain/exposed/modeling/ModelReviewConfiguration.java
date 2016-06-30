package com.latticeengines.domain.exposed.modeling;

import java.util.List;

import com.latticeengines.domain.exposed.modelreview.DataRule;


public class ModelReviewConfiguration extends DataProfileConfiguration {

    private List<DataRule> dataRules;

    public List<DataRule> getDataRules() {
        return dataRules;
    }

    public void setDataRules(List<DataRule> dataRules) {
        this.dataRules = dataRules;
    }

}
