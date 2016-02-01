package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.common.exposed.jython.JythonEngine;

@SuppressWarnings("rawtypes")
public class JythonFunction extends BaseOperation implements Function {

    private static final Log log = LogFactory.getLog(JythonFunction.class);
    private static final long serialVersionUID = 7015322136073224137L;

    private static JythonEngine engine;

    private String packageName;
    private String moduleName;
    private String functionName;
    private Fields fieldsToApply;
    private List<Integer> paramList;
    private Class<?> targetFieldType;

    public JythonFunction(String packageName, //
            String moduleName, //
            String functionName, //
            Fields fieldsToApply, //
            Fields fieldsDeclaration, //
            Class<?> targetFieldType) {
        super(0, fieldsDeclaration);
        this.packageName = packageName;
        this.moduleName = moduleName;
        this.functionName = functionName;
        this.fieldsToApply = fieldsToApply;
        this.targetFieldType = targetFieldType;
    }

    private List<Integer> computeParamList(Fields declaration, Fields argFields) {
        paramList = new ArrayList<>();
        for (int j = 0; j < declaration.size(); j++) {
            String declarationFieldName = (String) declaration.get(j);
            for (int i = 0; i < argFields.size(); i++) {
                String fieldName = (String) argFields.get(i);
                if (declarationFieldName.equals(fieldName)) {
                    paramList.add(i);
                }
            }

        }
        return paramList;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        if (engine == null) {
            log.info(String.format("Constructing jython engine."));
            engine = new JythonEngine(null);
        }

        Fields argFields = functionCall.getArgumentFields();
        if (paramList == null) {
            paramList = computeParamList(fieldsToApply, argFields);
        }
        String functionCallStr = functionName;
        List<Object> params = new ArrayList<>();

        TupleEntry entry = functionCall.getArguments();

        for (Integer paramIndex : paramList) {
            Object value = entry.getTuple().getObject(paramIndex);
            params.add(value);
        }
        functionCall.getOutputCollector().add(new Tuple( //
                engine.invoke(packageName, moduleName, functionCallStr, params.toArray(), targetFieldType)));
    }

}
