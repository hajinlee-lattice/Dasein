package com.latticeengines.objectapi.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
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

    @PostMapping(value = "/count")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Long getCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
                         @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return entityQueryService.getCount(frontEndQuery, version);
    }

    @PostMapping(value = "/data")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
                            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return entityQueryService.getData(frontEndQuery, version);
    }

    @PostMapping(value = "/ratingcount")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Map<String, Long> getRatingCount(@PathVariable String customerSpace,
                                            @RequestBody RatingEngineFrontEndQuery frontEndQuery,
                                            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return entityQueryService.getRatingCount(frontEndQuery, version);
    }

}
