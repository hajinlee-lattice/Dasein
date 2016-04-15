package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@DiscriminatorValue("RandomForest")
public class RandomForestAlgorithm extends AlgorithmBase {

    public RandomForestAlgorithm() {
        setName("RF");
        setScript("/app/dataplatform/scripts/algorithm/rf_train.py");
        setAlgorithmProperties(StringUtils.join(getAlgorithmPropertyArray(), " "));
        setPipelineDriver("/app/dataplatform/scripts/pipeline.json");
        setPipelineScript("/app/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/app/dataplatform/scripts/lepipeline.tar.gz");
    }
    
    @JsonIgnore
    @Transient
    private String[] getAlgorithmPropertyArray() {
        return new String[] { //
                "criterion=gini", //
                "n_estimators=100", //
                "n_jobs=5", //
                "min_samples_split=25", //
                "min_samples_leaf=10", //
                "max_depth=8", //
                "bootstrap=True" //
        };
    }
}
