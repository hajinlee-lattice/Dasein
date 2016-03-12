package com.latticeengines.common.exposed.jython;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.python.core.PyBoolean;
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
        PySystemState sys = new PySystemState();
        sys.path.append(new PyString(modelPath));
        interpreter = new PythonInterpreter(null, sys);
        interpreter.exec(String.format("import os;os.chdir('%s')", modelPath));
    }

    public PythonInterpreter getInterpreter() {
        return interpreter;
    }

    public Object invoke(String packageName, String module, String function, Object[] params, Class<?> returnType) {
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
        } else if (x instanceof PyBoolean) {
            return ((PyBoolean) x).getBooleanValue();
        } else if (x instanceof PyInteger) {
            if (returnType == Long.class) {
                return (long) ((PyInteger) x).getValue();
            }
            return ((PyInteger) x).getValue();
        } else if (x instanceof PyLong) {
            Object value = ((PyLong) x).getValue();
            if (value instanceof BigInteger) {
                if (returnType == Long.class) {
                    return ((BigInteger) value).longValue();
                } else if (returnType == Integer.class) {
                    return ((BigInteger) value).intValue();
                }
            }
            return ((PyLong) x).getValue().longValue();
        } else if (x instanceof PyDictionary) {
            return ((PyDictionary) x).__tojava__(Map.class);
        }
        return null;
    }

    public <T> T invoke(String function, Map<String, Object> arguments, Map<String, Object> record, Class<T> returnType) {
        
        String threadId = "x" + Thread.currentThread().getId();
        String script = functionScriptMap.get(function + "-" + threadId);

        if (script == null) {
            script = String.format("import %s; %s = %s.transform(args, record)", function, threadId, function);
            functionScriptMap.put(function + "-" + threadId, script);
        }
        interpreter.set("args", arguments);
        interpreter.set("record", record);
        interpreter.exec(script);
        PyObject x = interpreter.get(threadId);
        if (x instanceof PyFloat) {
            return returnType.cast(((PyFloat) x).getValue());
        } else if (x instanceof PyString) {
            return returnType.cast(x.toString());
        } else if (x instanceof PyInteger) {
            Integer value = ((PyInteger) x).getValue();
            if (returnType == Long.class) {
                return returnType.cast(Long.valueOf(value));
            }
            if (returnType == Double.class) {
                return returnType.cast(Double.valueOf((double) value));
            }
            return returnType.cast(((PyInteger) x).getValue());
        } else if (x instanceof PyLong) {
            Long value = ((PyLong) x).getValue().longValue();
            if (returnType == Double.class) {
                return returnType.cast(Double.valueOf((double) value));
            }
            return returnType.cast(value);
        }

        return null;
    }

}
