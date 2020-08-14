package com.latticeengines.app.exposed.controller;

import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.proxy.exposed.matchapi.ContactMetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "contact-metadata")
@RestController
@RequestMapping("/contact-metadata")
public class ContactMetadataResource {

    @Inject
    private ContactMetadataProxy contactMetadataProxy;

    @GetMapping("/tps-job-functions")
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<String> getTpsJobFunctions(HttpServletRequest request) {
        return contactMetadataProxy.getTpsJobFunctions();
    }

    @GetMapping("/tps-job-titles")
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<String> getTpsTitles(HttpServletRequest request) {
        return contactMetadataProxy.getTpsTitles();
    }
}
