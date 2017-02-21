package com.latticeengines.queryapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.network.exposed.queryapi.QueryInterface;

import io.swagger.annotations.Api;

@Api(value = "query", description = "REST resource for query")
@RestController
@RequestMapping("/querys")
public class QueryResource implements QueryInterface {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(QueryResource.class);

}
