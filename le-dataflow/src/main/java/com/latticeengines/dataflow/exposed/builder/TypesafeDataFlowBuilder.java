package com.latticeengines.dataflow.exposed.builder;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public abstract class TypesafeDataFlowBuilder<T extends DataFlowParameters> extends CascadingDataFlowBuilder {
    private static final Logger log = LoggerFactory.getLogger(TypesafeDataFlowBuilder.class);

    public void validate(T parameters) {
    }

    public abstract Node construct(T parameters);

    @Override
    @SuppressWarnings("unchecked")
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        T casted = null;
        try {
            casted = (T) parameters;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Expected parameters to be of type %s but are of type %s", //
                    classT().getName(), parameters.getClass().getName()));
        }

        try {
            validate(casted);
        } catch (Exception e) {
            throw new RuntimeException("Flow failed validations", e);
        }
        String confStr = JsonUtils.serialize(casted);
        if (StringUtils.isBlank(confStr) || confStr.length() < 2000) {
            log.info(String.format("Running flow with the following parameters: %s", confStr));
        } else {
            log.info(String.format("Running flow with the following big parameters: %s (truncated)", confStr.substring(0, 2000)));
        }
        return construct(casted);
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        throw new IllegalStateException("Not supported");
    }

    @SuppressWarnings("unchecked")
    private Class<T> classT() {
        Type[] typeArguments = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments();
        Type type = typeArguments[0];
        return (Class<T>) type;
    }

}
