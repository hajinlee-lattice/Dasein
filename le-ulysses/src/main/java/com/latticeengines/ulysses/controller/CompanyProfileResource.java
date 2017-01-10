package com.latticeengines.ulysses.controller;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.ulysses.service.CompanyProfileService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "companyprofile", description = "REST resource for company profiles")
@RestController
@RequestMapping("/companyprofiles/")
public class CompanyProfileResource {

    private static final Logger log = Logger.getLogger(CompanyProfileResource.class);

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    public CompanyProfile getCompanyProfile(HttpServletRequest request) {
        CustomerSpace space = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        Map<MatchKey, String> attributes = getAccountAttributes(request);
        log.info(String.format("Retrieving company profile for %s, attributes = [%s]", space, attributes));
        return null;
    }

    private Map<MatchKey, String> getAccountAttributes(HttpServletRequest request) {
        Enumeration<String> parameterNames = request.getParameterNames();
        Map<MatchKey, String> matchParameters = new HashMap<>();

        while (parameterNames.hasMoreElements()) {
            String parameterName = parameterNames.nextElement();
            MatchKey matchKey = getMatchKey(parameterName);
            matchParameters.put(matchKey, request.getParameter(parameterName));
        }
        return matchParameters;
    }

    private MatchKey getMatchKey(String parameterName) {
        try {
            MatchKey key = Enum.valueOf(MatchKey.class, parameterName);
            return key;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { parameterName });
        }
    }
}
