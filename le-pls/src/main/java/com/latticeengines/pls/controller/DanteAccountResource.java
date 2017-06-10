package com.latticeengines.pls.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.proxy.exposed.dante.DanteAccountProxy;
import com.wordnik.swagger.annotations.Api;

import io.swagger.annotations.ApiOperation;

@Api(value = "danteaccounts", description = "REST resource for Dante Accounts")
@RestController
@RequestMapping("/danteaccounts")
public class DanteAccountResource {
    private static final Logger log = Logger.getLogger(DanteAccountResource.class);

    @Autowired
    DanteAccountProxy danteAccountProxy;

    @RequestMapping(value = "/{count}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<List<DanteAccount>> getAccounts(@PathVariable int count) {
        return danteAccountProxy.getAccounts(count);
    }
}
