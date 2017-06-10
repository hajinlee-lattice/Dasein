package com.latticeengines.dante.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.AccountService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.network.exposed.dante.DanteAccountInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Dante Account operations")
@RestController
@RequestMapping("/accounts")
public class AccountResource implements DanteAccountInterface {
    private static final Logger log = Logger.getLogger(AccountResource.class);

    @Autowired
    AccountService accountService;

    @RequestMapping(value = "/{count}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get Dante accounts")
    @PreAuthorize("hasRole('View_PLS_PLAYS')")
    public ResponseDocument<List<DanteAccount>> getAccounts(@PathVariable int count) {
        return ResponseDocument.successResponse(accountService.getAccounts(count));
    }
}
