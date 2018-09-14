package com.latticeengines.security.exposed.jwt.handler;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;

public interface JwtHandler {
    public void register();

    public String getName();

    public String getJwtTokenWithRedirectURL(HttpServletRequest request, GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

    public String getJwtToken(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

    public void validateJwtParameters(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException;

}
