package com.latticeengines.pls.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.service.SalesforceURLService;
import io.swagger.annotations.Api;

@Api(value = "salesforce", description = "Redirect to Salesforce")
@RestController
@RequestMapping(value = "/salesforce")
public class SalesforceResource {
    @Autowired
    private SalesforceURLService salesforceURLService;

    @RequestMapping(value = "/bis-lp", method=RequestMethod.GET)
    @ResponseBody
    public void bisLP(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisLP();
        response.sendRedirect(url);
    }

    @RequestMapping(value = "/bis-lp-sandbox", method=RequestMethod.GET)
    @ResponseBody
    public void bisLPSandBox(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisLPSandbox();
        response.sendRedirect(url);
    }

    @RequestMapping(value = "/bis-ap", method=RequestMethod.GET)
    @ResponseBody
    public void bisAP(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisAP();
        response.sendRedirect(url);
    }

    @RequestMapping(value = "/bis-ap-sandbox", method=RequestMethod.GET)
    @ResponseBody
    public void bisAPSandBox(HttpServletResponse response) throws IOException
    {
        String url = salesforceURLService.getBisAPSandbox();
        response.sendRedirect(url);
    }
}
