package com.latticeengines.eai.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.network.exposed.eai.EaiJobDetailInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "eaijobdetail", description = "REST resource for get eai job details")
@RestController
@RequestMapping("")
public class EaiJobDetailResource implements EaiJobDetailInterface {

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Override
    @RequestMapping(value = "/jobdetail/collectionIdentifier/{collectionIdentifier:.+}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get an eai job detail")
    public EaiImportJobDetail getImportJobDetailByCollectionIdentifier(@PathVariable String collectionIdentifier) {
        return eaiImportJobDetailService.getImportJobDetailByCollectionIdentifier(collectionIdentifier);
    }

    @Override
    @RequestMapping(value = "/jobdetail/applicationId/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get an eai job detail")
    public EaiImportJobDetail getImportJobDetailByAppId(@PathVariable String applicationId) {
        return eaiImportJobDetailService.getImportJobDetailByAppId(applicationId);
    }

    @Override
    @RequestMapping(value = "/jobdetail/update", method = RequestMethod.POST, headers = "Accept=application/json")
    public void updateImportJobDetail(@RequestBody EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailService.updateImportJobDetail(eaiImportJobDetail);
    }

    @Override
    @RequestMapping(value = "/jobdetail/{collectionIdentifier:.+}/cancel", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Cancel eai job by identifier.")
    public void cancelImportJob(@PathVariable String collectionIdentifier) {
        eaiImportJobDetailService.cancelImportJob(collectionIdentifier);
    }

}
