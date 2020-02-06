package com.latticeengines.pls.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.VdbMetadataConstants;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "vdb-metadata")
@RestController
@RequestMapping(value = "/vdbmetadata")
public class VdbMetadataResource {

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
