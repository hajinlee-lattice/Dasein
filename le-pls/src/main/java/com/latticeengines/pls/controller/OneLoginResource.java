package com.latticeengines.pls.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.security.exposed.service.OneLoginService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "One Login Resources")
@RestController
@RequestMapping(path = "/onelogin")
public class OneLoginResource {

    private static final Logger log = LoggerFactory.getLogger(OneLoginResource.class);

    @Inject
    private OneLoginService oneLoginService;

    @GetMapping(path = "/metadata")
    @ResponseBody
    @ApiOperation(value = "Metadata or Audience of the SAML Service Provider")
    public String getSPMetadata() {
        return oneLoginService.getSPMetadata();
    }

    @PostMapping(path = "/acs/{profile}", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    @ApiOperation(value = "Assertion Consumer Service of the SAML Service Provider")
    public RedirectView processACS(@PathVariable("profile") String profile, HttpServletRequest request,
            HttpServletResponse response) {
        RedirectView redirectView = new RedirectView();
        String redirectUrl = oneLoginService.processACS(profile, request, response);
        log.info("Redirect OneLogin SAML ACS to " + redirectUrl);
        redirectView.setUrl(redirectUrl);
        return redirectView;
    }

    @GetMapping(path = "/http-redirect/slo/{profile}")
    @ResponseBody
    @ApiOperation(value = "Single Logout Service of the SAML Service Provider")
    public RedirectView processSLO(@PathVariable("profile") String profile, HttpServletRequest request,
                                   HttpServletResponse response) {
        RedirectView redirectView = new RedirectView();
        String redirectUrl = oneLoginService.processSLO(profile, request, response);
        log.info("Redirect OneLogin SLO to " + redirectUrl);
        redirectView.setUrl(redirectUrl);
        return redirectView;
    }

    @GetMapping(path = "/login")
    @ResponseBody
    @ApiOperation(value = "Metadata or Audience of the SAML Service Provider")
    public ResponseDocument<String> login(@RequestParam(name = "redirectTo", required = false) String redirectTo,
                                      HttpServletRequest request, HttpServletResponse response) {
        String redirectUrl = oneLoginService.login(redirectTo, request, response);
        log.info("Redirect OneLogin SAML login to " + redirectUrl);
        return ResponseDocument.successResponse(redirectUrl);
    }

}
