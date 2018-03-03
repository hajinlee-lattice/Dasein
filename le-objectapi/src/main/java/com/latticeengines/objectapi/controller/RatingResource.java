package com.latticeengines.objectapi.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.service.RatingQueryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Api(value = "ratings", description = "REST resource for ratings")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/rating")
public class RatingResource {

    private final RatingQueryService ratingQueryService;

    @Inject
    public RatingResource(RatingQueryService ratingQueryService) {
        this.ratingQueryService = ratingQueryService;
    }

    @PostMapping(value = "/count")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
                         @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return ratingQueryService.getCount(frontEndQuery, version);
    }

    @PostMapping(value = "/data", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Flux<DataPage> getData(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
                                  @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return ratingQueryService.getData(frontEndQuery, version);
        }).flux();
    }

    @PostMapping(value = "/coverage")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Map<String, Long> getRatingCount(@PathVariable String customerSpace,
                                            @RequestBody FrontEndQuery frontEndQuery,
                                            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return ratingQueryService.getRatingCount(frontEndQuery, version);
    }

}
