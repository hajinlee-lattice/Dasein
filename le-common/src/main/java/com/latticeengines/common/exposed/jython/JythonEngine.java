package com.latticeengines.common.exposed.jython;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

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
        PySystemState pystate = new PySystemState();
        pystate.path.add(modelPath);
        interpreter = new PythonInterpreter(new PyDictionary(), pystate);
        interpreter.exec(String.format("import os;os.chdir('%s')", modelPath));
    }
    
    public PythonInterpreter getInterpreter() {
        return interpreter;
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
            if (returnType == BigInteger.class) {
                return returnType.cast(BigInteger.valueOf(value));
            }
            return returnType.cast(((PyInteger) x).getValue());
        } else if (x instanceof PyLong) {
            return returnType.cast(((PyLong) x).getValue());
        }

        return null;
    }

}
