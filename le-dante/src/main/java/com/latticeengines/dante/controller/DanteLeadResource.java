package com.latticeengines.dante.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.DanteLeadService;
import com.latticeengines.domain.exposed.dante.DanteLeadDTO;
import com.latticeengines.network.exposed.dante.DanteLeadInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Dante Lead operations")
@RestController
@RequestMapping("/leads")
public class DanteLeadResource implements DanteLeadInterface {

    @Autowired
    private DanteLeadService danteLeadService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get Dante accounts")
    public void create(@RequestBody DanteLeadDTO danteLeadDTO, @RequestParam("customerSpace") String customerSpace) {
        danteLeadService.create(danteLeadDTO, customerSpace);
    }
}
