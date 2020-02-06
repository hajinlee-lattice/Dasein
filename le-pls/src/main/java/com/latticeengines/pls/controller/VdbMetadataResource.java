package com.latticeengines.pls.controller;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import com.latticeengines.domain.exposed.pls.VdbMetadataConstants;
import com.latticeengines.security.exposed.service.SessionService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata in VisiDB")
@RestController
@RequestMapping(value = "/vdbmetadata")
public class VdbMetadataResource {

    private static final Logger log = LoggerFactory.getLogger(VdbMetadataResource.class);

    @Inject
    private SessionService sessionService;

    @GetMapping("/options")
    @ResponseBody
    @ApiOperation(value = "Get map of metadata attribute options")
    public Map<String, String[]> getOptions(HttpServletRequest request) {
        Map<String, String[]> map = new HashMap<String, String[]>();
        map.put("CategoryOptions", VdbMetadataConstants.CATEGORY_OPTIONS);
        map.put("ApprovedUsageOptions", VdbMetadataConstants.APPROVED_USAGE_OPTIONS);
        map.put("FundamentalTypeOptions", VdbMetadataConstants.FUNDAMENTAL_TYPE_OPTIONS);
        map.put("StatisticalTypeOptions", VdbMetadataConstants.STATISTICAL_TYPE_OPTIONS);
        return map;
    }
}
