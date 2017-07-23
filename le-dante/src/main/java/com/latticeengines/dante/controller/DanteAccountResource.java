package com.latticeengines.dante.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.DanteAccountService;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.network.exposed.dante.DanteAccountInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Dante Account operations")
@RestController
@RequestMapping("/accounts")
public class DanteAccountResource implements DanteAccountInterface {

    @Autowired
    private DanteAccountService danteAccountService;

    @RequestMapping(value = "/{count}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Dante accounts")
    public List<DanteAccount> getAccounts(@PathVariable int count,
            @RequestParam("customerSpace") String customerSpace) {
        return danteAccountService.getAccounts(count, customerSpace);
    }
}
