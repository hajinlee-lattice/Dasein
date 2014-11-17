package com.latticeengines.skald;

import java.util.Map;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.latticeengines.skald.model.FieldType;

public class PythonTransform {
    public PythonTransform(String python, FieldType type) {
        // TODO Consider having a shared instance of this.
        interpreter = new PythonInterpreter();

        interpreter.exec(python);
        function = (PyFunction) interpreter.get("transform");

        this.type = type;
    }

    public Object invoke(Map<String, Object> arguments, Map<String, Object> record) {
        PyDictionary pyArguments = new PyDictionary();
        if (arguments != null) {
            pyArguments.putAll(arguments);
        }

        PyDictionary pyRecord = new PyDictionary();
        pyRecord.putAll(record);

        PyObject result = function.__call__(pyArguments, pyRecord);

        switch (type) {
        case Boolean:
            return Py.py2boolean(result);
        case Float:
            return Py.py2double(result);
        case Integer:
            return Py.py2int(result);
        case String:
            return result.toString();
        case Temporal:
            return Py.py2long(result);
        }

        throw new RuntimeException("Unreached");
    }

    private final PythonInterpreter interpreter;
    private final PyFunction function;
    private final FieldType type;
}
