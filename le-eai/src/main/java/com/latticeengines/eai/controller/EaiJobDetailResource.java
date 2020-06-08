package com.latticeengines.eai.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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

    @Inject
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Override
    @GetMapping("/jobdetail/collectionIdentifier/{collectionIdentifier:.+}")
    @ApiOperation(value = "Get an eai job detail")
    public EaiImportJobDetail getImportJobDetailByCollectionIdentifier(@PathVariable String collectionIdentifier) {
        return eaiImportJobDetailService.getImportJobDetailByCollectionIdentifier(collectionIdentifier);
    }

    @Override
    @GetMapping("/jobdetail/applicationId/{applicationId}")
    @ApiOperation(value = "Get an eai job detail")
    public EaiImportJobDetail getImportJobDetailByAppId(@PathVariable String applicationId) {
        return eaiImportJobDetailService.getImportJobDetailByAppId(applicationId);
    }

    @Override
    @PostMapping("/jobdetail/update")
    public void updateImportJobDetail(@RequestBody EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailService.updateImportJobDetail(eaiImportJobDetail);
    }

    @Override
    @PostMapping("/jobdetail/{collectionIdentifier:.+}/cancel")
    @ApiOperation(value = "Cancel eai job by identifier.")
    public void cancelImportJob(@PathVariable String collectionIdentifier) {
        eaiImportJobDetailService.cancelImportJob(collectionIdentifier);
    }

}
