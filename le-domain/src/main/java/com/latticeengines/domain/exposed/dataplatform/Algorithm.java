package com.latticeengines.domain.exposed.dataplatform;

import java.util.Properties;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.RandomForestAlgorithm;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LogisticRegressionAlgorithm.class, name = "logisticRegressionAlgorithm"),
    @JsonSubTypes.Type(value = DecisionTreeAlgorithm.class, name = "decisionTreeAlgorithm"),
    @JsonSubTypes.Type(value = RandomForestAlgorithm.class, name = "randomForestAlgorithm")
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
