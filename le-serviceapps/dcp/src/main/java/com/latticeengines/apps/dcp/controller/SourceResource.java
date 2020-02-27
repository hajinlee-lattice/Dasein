package com.latticeengines.apps.dcp.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;

@Api(value = "Source", description = "REST resource for source")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/source")
public class SourceResource {


}
