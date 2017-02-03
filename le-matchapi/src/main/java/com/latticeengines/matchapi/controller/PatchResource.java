package com.latticeengines.matchapi.controller;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "patch", description = "REST resource for account master lookup patch")
@RestController
@RequestMapping("/patches")
public class PatchResource {

    @Autowired
    private PatchService patchService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Batch update a list of lookup entries", response = LookupUpdateResponse.class)
    private LookupUpdateResponse patch(@RequestBody List<LookupUpdateRequest> requests) {
        return patchService.patch(requests);
    }

}
