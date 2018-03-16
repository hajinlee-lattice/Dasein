package com.latticeengines.objectapi.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.proxy.exposed.objectapi.TransactionProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "transactions", description = "REST resource for transactions")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/transactions")
public class TransactionResource implements TransactionProxy {

    @Inject
    private TransactionService transactionService;

    @GetMapping(value = "/maxtransactiondate")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public String getMaxTransactionDate(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = true) DataCollection.Version version) {
        return transactionService.getMaxTransactionDate(version);
    }
}
