package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@DiscriminatorValue("DataReview")
public class DataReviewAlgorithm extends AlgorithmBase {

    public DataReviewAlgorithm() {
        setName("DATAREV");
        setScript("/app/dataplatform/scripts/algorithm/data_rule.py");
        setAlgorithmProperties("");
        setPipelineDriver("/app/dataplatform/scripts/rulepipeline.json");
        setPipelineScript("/app/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/app/dataplatform/scripts/lepipeline.tar.gz");
        setPipelineProperties(StringUtils.join(getPipelinePropertyArray(), " "));
    }

    @JsonIgnore
    @Transient
    private String[] getPipelinePropertyArray() {
        return new String[] { //
                "anonymousleadrule.threshold=50" //
        };
    }
}
