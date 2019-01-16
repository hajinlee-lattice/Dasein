package com.latticeengines.domain.exposed.modeling.algorithm;

import java.util.Random;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@DiscriminatorValue("RandomForest")
public class RandomForestAlgorithm extends AlgorithmBase {

    public RandomForestAlgorithm() {
        setName("RF");
        setScript("/datascience/dataplatform/scripts/algorithm/rf_train.py");
        setAlgorithmProperties(StringUtils.join(getAlgorithmPropertyArray(), " "));
        setPipelineDriver("/datascience/dataplatform/scripts/pipeline.json");
        setPipelineScript("/datascience/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/datascience/dataplatform/scripts/lepipeline.tar.gz");
        setPipelineProperties(StringUtils.join(getPipelinePropertyArray(), " "));
    }

    @Override
    public void resetAlgorithmProperties() {
        setAlgorithmProperties(StringUtils.join(getAlgorithmPropertyArray(), " "));
    }

    @JsonIgnore
    @Transient
    private String[] getAlgorithmPropertyArray() {
        return new String[] { //
                "criterion=gini", //
                "n_estimators=200", //
                "n_jobs=5", //
                "min_samples_split=25", //
                "min_samples_leaf=20", // Should use percentage when it's
                                       // available
                "max_depth=6", //
                "bootstrap=True", //
                "random_state=" + new Random().nextInt(100000) };
    }

    @JsonIgnore
    @Transient
    private String[] getPipelinePropertyArray() {
        return new String[] {};
    }
}
