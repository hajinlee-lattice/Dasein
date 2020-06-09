package com.latticeengines.objectapi.controller;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.monitor.exposed.annotation.InvocationMeter;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.query.factory.RedshiftQueryProvider;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for entities")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/entity")
public class EntityResource {

    private static final String SEGMENT_USER = RedshiftQueryProvider.USER_SEGMENT;
    private static final String BATCH_USER = RedshiftQueryProvider.USER_BATCH;

    private final EntityQueryService entityQueryService;

    @Inject
    public EntityResource(EntityQueryService entityQueryService) {
        this.entityQueryService = entityQueryService;
    }

    @PostMapping("/count")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name = "entity-count", measurment = "objectapi")
    public Long getCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestParam(value = "sqlUser", required = false) String sqlUser) {
        if (StringUtils.isBlank(sqlUser)) {
            sqlUser = SEGMENT_USER;
        }
        return entityQueryService.getCount(frontEndQuery, version, sqlUser);
    }

    @PostMapping("/data")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestParam(value = "sqlUser", required = false) String sqlUser,
            @RequestParam(value = "enforceTranslation", required = false, defaultValue = "false") Boolean enforceTranslation) {
        final String finalSqlUser = StringUtils.isBlank(sqlUser) ? BATCH_USER : sqlUser;
        return entityQueryService.getData(frontEndQuery, version, finalSqlUser, enforceTranslation);

    }

    @PostMapping("/query")
    @ResponseBody
    @ApiOperation(value = "Retrieve the SQL for the specified query")
    public String getQuery(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestParam(value = "enforceTranslation", required = false, defaultValue = "false") Boolean enforceTranslation,
            @RequestParam(value = "isCountQuery", required = false, defaultValue = "false") Boolean isCountQuery) {
        return entityQueryService.getQueryStr(frontEndQuery, version, BATCH_USER, isCountQuery);
    }

    @PostMapping("/ratingcount")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rating count for the specified query")
    @InvocationMeter(name ="entity-rating-coverage", measurment = "objectapi")
    public Map<String, Long> getRatingCount(@PathVariable String customerSpace,
            @RequestBody RatingEngineFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestParam(value = "sqlUser", required = false) String sqlUser) {
        if (StringUtils.isBlank(sqlUser)) {
            sqlUser = SEGMENT_USER;
        }
        return entityQueryService.getRatingCount(frontEndQuery, version, sqlUser);
    }

}
