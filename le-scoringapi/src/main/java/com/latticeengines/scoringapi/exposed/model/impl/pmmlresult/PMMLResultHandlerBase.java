package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.HashMap;
import java.util.Map;

public abstract class PMMLResultHandlerBase implements PMMLResultHandler {

    private static Map<Class<?>, PMMLResultHandler> handlers = new HashMap<>();
    
    public PMMLResultHandlerBase(Class<?>[] resultTypes) {
        for (Class<?> c : resultTypes) {
            handlers.put(c, this);
        }
    }
    
    public static PMMLResultHandler getHandler(Class<?> resultType) {
        return handlers.get(resultType);
    }
}
