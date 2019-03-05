package com.latticeengines.security.exposed.jwt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JwtReplyParameters;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;
import com.latticeengines.security.exposed.jwt.handler.JwtHandler;

@Component("jwtManager")
public class JwtManager {

    static final String SOURCE_REF_KEY = "source_ref";

    private static final Map<String, JwtHandler> jwtTokenHandlers = new HashMap<>();

    private JwtHandler getJwtTokenHandler(String sourceRef) {
        return jwtTokenHandlers.get(sourceRef);
    }

    public static void registerTokenHandler(String sourceRef, JwtHandler jwtGenerator) {
        jwtTokenHandlers.put(sourceRef, jwtGenerator);
    }

    public JwtReplyParameters handleJwtRequest(GlobalAuthUser user, JwtRequestParameters reqParameters)
            throws LedpException {
        Map<String, String> parameters = reqParameters.getRequestParameters();
        if (parameters.containsKey(SOURCE_REF_KEY)) {
            if (StringUtils.isBlank(parameters.get(SOURCE_REF_KEY))) {
                throw new LedpException(LedpCode.LEDP_19012);
            }
        } else {
            throw new LedpException(LedpCode.LEDP_10004, new String[] { SOURCE_REF_KEY });
        }
        String ref = parameters.get(SOURCE_REF_KEY).toUpperCase();
        JwtHandler jwtHandler = getJwtTokenHandler(ref);
        if (jwtHandler == null) {
            throw new LedpException(LedpCode.LEDP_19007, new String[] { ref });
        }
        JwtReplyParameters reply;
        try {
            jwtHandler.validateJwtParameters(user, reqParameters);
            reply = jwtHandler.getJwtTokenWithFunction(user, reqParameters);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19006, e);
        }
        return reply;
    }
}
