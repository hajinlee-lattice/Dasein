package com.latticeengines.dataflow.runtime.cascading;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.script.ScriptException;

import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.common.exposed.jython.JythonEvaluator;

@SuppressWarnings("rawtypes")
public class JythonFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7015322136073224137L;
    private static JythonEvaluator evaluator;
    
    
    static {
        if (evaluator == null) {
            evaluator = new JythonEvaluator();
            URL url = ClassLoader.getSystemResource("pythonlib");
            File pythonLibDir = new File(url.getFile());
            String[] paths = pythonLibDir.list();
            int i = 0;
            for (String path : paths) {
                paths[i] = pythonLibDir.getAbsolutePath() + "/" + path;
            }
            evaluator.initialize(paths);
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
        String functionCallStr = functionName + "(";
        
        TupleEntry entry = functionCall.getArguments();

        boolean first = true;
        for (Integer paramIndex : paramList) {
            if (!first) {
                functionCallStr += ",";
            }
            Object value = entry.getTuple().getObject(paramIndex);
            if (value instanceof String) {
                value = "'" + value.toString() + "'";
            } else {
                value = value.toString();
            }
            functionCallStr += value;
            first = false;
        }
        functionCallStr += ")";
        
        try {
            functionCall.getOutputCollector().add(new Tuple(evaluator.execute(functionCallStr, returnType)));
        } catch (ScriptException e) {
            throw new FlowException(e);
        }
    }

}
