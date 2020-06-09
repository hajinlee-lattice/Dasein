package com.latticeengines.app.exposed.controller;

import static com.latticeengines.domain.exposed.exception.LedpCode.LEDP_18152;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataCloudService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacloud", description = "REST resource for source datacloud")
@RestController
@RequestMapping("/datacloud")
public class DataCloudResource {

    private final DataCloudService dataCloudService;

    @Inject
    public DataCloudResource(DataCloudService dataCloudService) {
        this.dataCloudService = dataCloudService;
    }

    @PostMapping("/customerreports/incorrectlookups")
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public ResponseDocument<String> createLookupCustomerReport(@RequestBody IncorrectLookupReportRequest request) {
        try {
            CustomerReport report = dataCloudService.reportIncorrectLookup(request);
            return ResponseDocument.successResponse(report.getId());
        } catch (Exception e) {
            throw new LedpException(LEDP_18152);
        }
    }

    @PostMapping("/customerreports/incorrectmatchedattrs")
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public ResponseDocument<String> createMatchedAttrsCustomerReport(@RequestBody IncorrectMatchedAttrReportRequest request) {
        try {
            CustomerReport report = dataCloudService.reportIncorrectMatchedAttr(request);
            return ResponseDocument.successResponse(report.getId());
        } catch (Exception e) {
            throw new LedpException(LEDP_18152);
        }
    }
}
