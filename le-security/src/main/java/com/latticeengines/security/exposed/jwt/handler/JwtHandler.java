package com.latticeengines.security.exposed.jwt.handler;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JwtReplyParameters;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;

public interface JwtHandler {
    void register();

    String getName();

    JwtReplyParameters getJwtTokenWithFunction(GlobalAuthUser user, JwtRequestParameters reqParameters)
            throws LedpException;

    String getJwtToken(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

    void validateJwtParameters(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

}
