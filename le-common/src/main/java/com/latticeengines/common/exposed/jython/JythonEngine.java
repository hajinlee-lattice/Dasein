package com.latticeengines.common.exposed.jython;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.python.core.PyDictionary;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

public class JythonEngine {

    private PythonInterpreter interpreter;
    private Map<String, String> functionScriptMap = new HashMap<>();
    
    public JythonEngine(String modelPath) {
        if (modelPath == null) {
            interpreter = new PythonInterpreter();
            return;
        }
        PySystemState pystate = new PySystemState();
        pystate.path.add(modelPath);
        interpreter = new PythonInterpreter(new PyDictionary(), pystate);
        interpreter.exec(String.format("import os;os.chdir('%s')", modelPath));
    }
    
    public PythonInterpreter getInterpreter() {
        return interpreter;
    }
    
    public Object invoke(String packageName, String module, String function, Object[] params) {
        String script = functionScriptMap.get(packageName + "." + module + "." + function);
        if (script == null) {
            List<String> l = new ArrayList<>();
            for (int i = 1; i <= params.length; i++) {
                l.add("p" + i);
            }
            
            script = String.format("from %s import %s; x = %s.%s(%s)", //
                    packageName, module, module, function, StringUtils.join(l, ","));
            functionScriptMap.put(packageName + "." + module + "." + function, script);
        }
        
        for (int i = 1; i <= params.length; i++) {
            interpreter.set("p" + i, params[i - 1]);
        }
        interpreter.exec(script);
        PyObject x = interpreter.get("x");
        if (x instanceof PyFloat) {
            return ((PyFloat) x).getValue();
        } else if (x instanceof PyString) {
            return x.toString();
        } else if (x instanceof PyInteger) {
            return ((PyInteger) x).getValue();
        } else if (x instanceof PyLong) {
            return ((PyLong) x).getValue().longValue();
        }
        return null;
    }
    
    public <T> T invoke(String function, Map<String, Object> arguments, Map<String, Object> record, Class<T> returnType) {
        String script = functionScriptMap.get(function);
        
        if (script == null) {
            script = String.format("import %s; x = %s.transform(args, record)", function, function);
            functionScriptMap.put(function, script);
        }
        interpreter.set("args", arguments);
        interpreter.set("record", record);
        interpreter.exec(script);
        PyObject x = interpreter.get("x");
        if (x instanceof PyFloat) {
            return returnType.cast(((PyFloat) x).getValue());
        } else if (x instanceof PyString) {
            return returnType.cast(x.toString());
        } else if (x instanceof PyInteger) {
            Integer value = ((PyInteger) x).getValue();
            if (returnType == Long.class) {
                return returnType.cast(Long.valueOf(value));
            }
            return returnType.cast(((PyInteger) x).getValue());
        } else if (x instanceof PyLong) {
            return returnType.cast(((PyLong) x).getValue().longValue());
        }

        return null;
    }

}
