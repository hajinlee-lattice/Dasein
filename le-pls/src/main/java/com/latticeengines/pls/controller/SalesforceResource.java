package com.latticeengines.pls.controller;

import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.service.SalesforceURLService;

import io.swagger.annotations.Api;

@Api(value = "salesforce", description = "Redirect to Salesforce")
@RestController
@RequestMapping("/salesforce")
public class SalesforceResource {
    @Inject
    private SalesforceURLService salesforceURLService;

    @GetMapping("/bis-lp")
    @ResponseBody
    public void bisLP(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisLP();
        response.sendRedirect(url);
    }

    @GetMapping("/bis-lp-sandbox")
    @ResponseBody
    public void bisLPSandBox(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisLPSandbox();
        response.sendRedirect(url);
    }

    @GetMapping("/bis-ap")
    @ResponseBody
    public void bisAP(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisAP();
        response.sendRedirect(url);
    }

    @GetMapping("/bis-ap-sandbox")
    @ResponseBody
    public void bisAPSandBox(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisAPSandbox();
        response.sendRedirect(url);
    }
}
