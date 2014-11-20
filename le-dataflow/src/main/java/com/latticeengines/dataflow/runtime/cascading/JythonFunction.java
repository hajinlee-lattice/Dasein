package com.latticeengines.dataflow.runtime.cascading;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.common.exposed.jython.JythonEvaluator;
import com.latticeengines.common.exposed.util.JarUtils;

@SuppressWarnings("rawtypes")
public class JythonFunction extends BaseOperation implements Function {
    private static final Log log = LogFactory.getLog(JythonFunction.class);
    private static final long serialVersionUID = 7015322136073224137L;
    private static JythonEvaluator evaluator;

    static {
        if (evaluator == null) {
            
            String[] paths = new String[] {};
            try {
                paths = JarUtils.getResourceListing(JythonFunction.class, "pythonlib");
            } catch (URISyntaxException | IOException e) {
                log.error(ExceptionUtils.getFullStackTrace(e));
            }
            List<String> pyPaths = new ArrayList<>();
            for (String path : paths) {
                if (path.endsWith(".py")) {
                    pyPaths.add(path);
                }
            }
            paths = new String[pyPaths.size()];
            pyPaths.toArray(paths);
            evaluator = JythonEvaluator.fromResource(paths);
        }

    }
    private String functionName;
    private Class<?> returnType;
    private Fields fieldsToApply;
    private List<Integer> paramList;

    public JythonFunction(String functionName, Class<?> returnType, Fields fieldsToApply, Fields fieldsDeclaration) {
        super(0, fieldsDeclaration);
        this.functionName = functionName;
        this.fieldsToApply = fieldsToApply;
        this.returnType = returnType;
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
        functionCall.getOutputCollector().add(new Tuple(evaluator.function(functionCallStr, returnType, params.toArray())));
    }

}
