package com.latticeengines.security.exposed.jwt.handler;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JwtReplyParameters;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;

public interface JwtHandler {
    public void register();

    public String getName();

    public JwtReplyParameters getJwtTokenWithFunction(GlobalAuthUser user, JwtRequestParameters reqParameters)
            throws LedpException;

    public String getJwtToken(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

    public void validateJwtParameters(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

}
