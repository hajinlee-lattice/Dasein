package com.latticeengines.dataflow.exposed.builder;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public abstract class TypesafeDataFlowBuilder<T extends DataFlowParameters> extends CascadingDataFlowBuilder {

    public abstract Node construct(T parameters);

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        T casted = null;
        try {
            casted = (T) parameters;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Expected parameters to be of type %s but are of type %s", //
                    classT().getName(), parameters.getClass().getName()));
        }

        return construct(casted);
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        throw new IllegalStateException("Not supported");
    }

    private Class<T> classT() {
        Type[] typeArguments = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments();
        Type type = typeArguments[0];
        return (Class<T>) type;
    }

}
