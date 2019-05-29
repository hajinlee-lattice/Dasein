package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.db.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "graph", description = "REST resource for graph")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/graph")
public class GraphResource {

    private static final Logger log = LoggerFactory.getLogger(GraphResource.class);

    @Inject
    private GraphVisitor graphVisitor;

    @RequestMapping(value = "/populate-graph", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Populate graph for a tenant")
    public void populateGraph( //
            @PathVariable String customerSpace) throws Exception {
        graphVisitor.populateTenantGraph(MultiTenantContext.getTenant());
    }
}
