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
import com.latticeengines.objectapi.service.RatingQueryService;
import com.latticeengines.query.factory.RedshiftQueryProvider;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratings", description = "REST resource for ratings")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/rating")
public class RatingResource {

    private final RatingQueryService ratingQueryService;

    private static final String SEGMENT_USER = RedshiftQueryProvider.USER_SEGMENT;
    private static final String BATCH_USER = RedshiftQueryProvider.USER_BATCH;

    @Inject
    public RatingResource(RatingQueryService ratingQueryService) {
        this.ratingQueryService = ratingQueryService;
    }

    @PostMapping(value = "/count")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Long getCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
                         @RequestParam(value = "sqlUser", required = false) String sqlUser) {
        if (StringUtils.isBlank(sqlUser)) {
            sqlUser = SEGMENT_USER;
        }
        return ratingQueryService.getCount(frontEndQuery, version, sqlUser);
    }

    @PostMapping(value = "/data")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
                                  @RequestParam(value = "sqlUser", required = false) String sqlUser) {
        final String finalSqlUser = StringUtils.isBlank(sqlUser) ? BATCH_USER : sqlUser;
        return ratingQueryService.getData(frontEndQuery, version, finalSqlUser);
    }

    @PostMapping(value = "/query")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public String getQuery(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery,
                           @RequestParam(value = "version", required = false) DataCollection.Version version,
                           @RequestParam(value = "enforceTranslation", required = false, defaultValue = "false") Boolean enforceTranslation) {
        return ratingQueryService.getQueryStr(frontEndQuery, version, BATCH_USER);
    }

    @PostMapping(value = "/coverage")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Map<String, Long> getRatingCount(@PathVariable String customerSpace,
            @RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
                                            @RequestParam(value = "sqlUser", required = false) String sqlUser) {
        if (StringUtils.isBlank(sqlUser)) {
            sqlUser = SEGMENT_USER;
        }
        return ratingQueryService.getRatingCount(frontEndQuery, version, sqlUser);
    }

}
