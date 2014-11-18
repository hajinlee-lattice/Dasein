package com.latticeengines.common.exposed.jython;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StreamUtils;

public class JythonEvaluator {
    
    private static final Log log = LogFactory.getLog(JythonEvaluator.class);

    private static ScriptEngine pyEngine;
    
    static {
        pyEngine = new ScriptEngineManager().getEngineByName("python");
    }
    
    public void initializeFromJar(String... paths) {
        for (String path : paths) {
            try {
                try {
                    String script = StreamUtils.copyToString(ClassLoader.getSystemResourceAsStream(path), Charset.defaultCharset());
                    pyEngine.eval(script);
                } catch (IOException e) {
                    log.warn("Cannot load script " + path, e);
                }
            } catch (ScriptException e) {
                log.error("Cannot evaluate python file: " + path);
                log.error(ExceptionUtils.getFullStackTrace(e));
            }
        }
    }
    
    public <T> T execute(String expression, Class<T> valueClass) throws ScriptException {
        pyEngine.eval("result = " + expression);
        Object result = pyEngine.get("result");
        if (result == null) {
            return null;
        }
        
        // TODO not pretty; need to come up with more generic solution
        if (result instanceof BigInteger) {
            if (valueClass == Integer.class) {
                result = ((BigInteger) result).intValue();
            } else if (valueClass == Long.class) {
                result = ((BigInteger) result).longValue();
            }
        }
        
        if (valueClass.isInstance(result)) {
            return valueClass.cast(result);
        } else {
            throw new RuntimeException("Value is not of type " + valueClass);
        }
        
    }
}
