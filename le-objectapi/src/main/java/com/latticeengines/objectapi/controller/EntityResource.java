package com.latticeengines.objectapi.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.objectapi.service.EntityQueryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for entities")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/entity")
public class EntityResource {

    private final EntityQueryService entityQueryService;

    @Autowired
    public EntityResource(EntityQueryService entityQueryService) {
        this.entityQueryService = entityQueryService;
    }

    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery) {
        return entityQueryService.getCount(frontEndQuery);
    }

    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery) {
        return entityQueryService.getData(frontEndQuery);
    }

    @RequestMapping(value = "/ratingcount", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Map<String, Long> getRatingCount(@PathVariable String customerSpace,
            @RequestBody FrontEndQuery frontEndQuery) {
        return entityQueryService.getRatingCount(frontEndQuery);
    }

}
