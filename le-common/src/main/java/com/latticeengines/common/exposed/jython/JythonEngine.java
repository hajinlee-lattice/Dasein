package com.latticeengines.common.exposed.jython;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.python.core.PyBoolean;
import org.python.core.PyCode;
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
    private Map<String, PyCode> functionScriptMap = new HashMap<>();

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
        PyCode pyCode = functionScriptMap.get(packageName + "." + module + "." + function);
        if (pyCode == null) {
            List<String> l = new ArrayList<>();
            for (int i = 1; i <= params.length; i++) {
                l.add("p" + i);
            }

            String script = String.format("from %s import %s; x = %s.%s(%s)", //
                    packageName,module, module, function, StringUtils.join(l, ","));
            pyCode = interpreter.compile(script);
            functionScriptMap.put(packageName + "." + module + "." + function, pyCode);
        }

        for (int i = 1; i <= params.length; i++) {
            interpreter.set("p" + i, params[i - 1]);
        }
        interpreter.exec(pyCode);
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
        PyCode pyCode = functionScriptMap.get(function + "-" + threadId);

        if (pyCode == null) {
            String script = String.format("import %s; %s = %s.transform(args, record)", function, threadId, function);
            pyCode = interpreter.compile(script);
            functionScriptMap.put(function + "-" + threadId, pyCode);
        }
        interpreter.set("args", arguments);
        interpreter.set("record", record);
        interpreter.exec(pyCode);
        PyObject x = interpreter.get(threadId);
        if (x instanceof PyFloat) {
            Double value = ((PyFloat) x).getValue();
            if (returnType == Long.class) {
                return returnType.cast(value.longValue());
            }
            if (returnType == Double.class) {
                return returnType.cast(Double.valueOf(value));
            }
            if (returnType == Boolean.class) {
                return returnType.cast(value == 1);
            }
            return returnType.cast(((PyFloat) x).getValue());

        } else if (x instanceof PyString) {
            return returnType.cast(x.toString());
        } else if (x instanceof PyInteger) {
            Integer value = ((PyInteger) x).getValue();
            if (returnType == Long.class) {
                return returnType.cast(Long.valueOf(value));
            }
            if (returnType == Double.class) {
                return returnType.cast(Double.valueOf(value));
            }
            if (returnType == Boolean.class) {
                return returnType.cast(value == 1);
            }
            return returnType.cast(((PyInteger) x).getValue());
        } else if (x instanceof PyLong) {
            Long value = ((PyLong) x).getValue().longValue();
            if (returnType == Double.class) {
                return returnType.cast(Double.valueOf(value));
            }
            return returnType.cast(value);
        }

        return null;
    }

}
