package com.latticeengines.security.exposed.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.auth.OneLoginExternalSession;

public interface OneLoginService {

    String getSPMetadata();

    String processACS(String profile, HttpServletRequest request, HttpServletResponse response);

    String processSLO(String profile, HttpServletRequest request, HttpServletResponse response);

    String login(String redirectTo, HttpServletRequest request, HttpServletResponse response);

    String logout(OneLoginExternalSession externalSession, String redirectTo);

}
