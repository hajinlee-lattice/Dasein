package com.latticeengines.dataplatform.exposed.domain;

import java.util.Properties;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.latticeengines.dataplatform.exposed.domain.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.RandomForestAlgorithm;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LogisticRegressionAlgorithm.class, name = "LogisticRegressionAlgorithm"),
    @JsonSubTypes.Type(value = DecisionTreeAlgorithm.class, name = "DecisionTreeAlgorithm"),
    @JsonSubTypes.Type(value = RandomForestAlgorithm.class, name = "RandomForestAlgorithm")
})
public interface Algorithm extends HasName {

    String getName();

    String getScript();

    String getContainerProperties();

    void setContainerProperties(String containerProperties);

    String getAlgorithmProperties();
    
    void setAlgorithmProperties(String algorithmProperties);

    Properties getAlgorithmProps();

    Properties getContainerProps();

    int getPriority();
    
    void setPriority(int priority);
    
    String getSampleName();
    
    void setSampleName(String sampleName);

}
