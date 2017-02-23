package com.latticeengines.objectapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.network.exposed.objectapi.QueryInterface;

import io.swagger.annotations.Api;

@Api(value = "query", description = "REST resource for query")
@RestController
@RequestMapping("/queries")
public class QueryResource implements QueryInterface {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(QueryResource.class);

}
