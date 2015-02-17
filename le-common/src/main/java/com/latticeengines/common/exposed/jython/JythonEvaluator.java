package com.latticeengines.common.exposed.jython;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.springframework.util.StreamUtils;

public class JythonEvaluator {
    // TODO Consider sharing this if instantiating many eventually causes
    // issues. That would require some mechanism for sandboxing, probably with
    // imports and aliasing.
    private final ScriptEngine engine = new ScriptEngineManager().getEngineByName("python");

    public static JythonEvaluator fromResource(String... paths) {
        String[] scripts = new String[paths.length];
        for (int index = 0; index < paths.length; index++) {
            try {
                scripts[index] = StreamUtils.copyToString(
                        JythonEvaluator.class.getClassLoader().getResourceAsStream(paths[index]),
                        Charset.defaultCharset());
            } catch (IOException e) {
                throw new RuntimeException("Unable to load file: " + paths[index], e);
            }
        }

        return new JythonEvaluator(scripts);
    }

    public JythonEvaluator(String... scripts) {
        for (String script : scripts) {
            try {
                engine.eval(script);
            } catch (ScriptException e) {
                throw new RuntimeException("Unable to load script", e);
            }
        }
    }

    public <T> T function(String name, Class<T> type, Object... arguments) {
        Invocable invocable = (Invocable) engine;

        Object result;
        try {
            result = invocable.invokeFunction(name, arguments);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to find function: " + name, e);
        } catch (ScriptException e) {
            throw new RuntimeException("Unable to execute function: " + name, e);
        }

        // TODO not pretty; need to come up with more generic solution
        if (result instanceof BigInteger) {
            if (type == Integer.class) {
                result = ((BigInteger) result).intValue();
            } else if (type == Long.class) {
                result = ((BigInteger) result).longValue();
            }
        }

        // Explicitly widen integers if required.
        if (type == Long.class && result instanceof Integer) {
            result = ((Integer) result).longValue();
        }

        // Explicitly widen floats if required.
        if (type == Double.class && result instanceof Float) {
            result = ((Float) result).doubleValue();
        }

        return type.cast(result);
    }
}
