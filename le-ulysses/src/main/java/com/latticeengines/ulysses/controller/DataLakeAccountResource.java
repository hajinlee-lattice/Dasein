package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.DataPage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "CDLAccounts", description = "Common REST resource to lookup CDL accounts")
@RestController
@RequestMapping("/datacollection/accounts")
public class DataLakeAccountResource {

    private final DataLakeService dataLakeService;

    @Inject
    public DataLakeAccountResource(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }

    @RequestMapping(value = "/{accountID}/{attributeGroup}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get an account by of attributes in a group")
    public DataPage getAttributesInPredefinedGroup(@PathVariable String accountID, //
            @PathVariable Predefined attributeGroup) {
        return dataLakeService.getAccountById(accountID, attributeGroup);
    }
}
