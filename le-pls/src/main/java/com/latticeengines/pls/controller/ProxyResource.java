package com.latticeengines.pls.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;

/**
 * This is temporary solution before migrating underlying services to service apps
 */
@Api(value = "proxy", description = "REST resource for proxied methods")
@RestController
@RequestMapping(value = "/internal/proxy/customerspaces/{customerSpace}")
public class ProxyResource {



}
