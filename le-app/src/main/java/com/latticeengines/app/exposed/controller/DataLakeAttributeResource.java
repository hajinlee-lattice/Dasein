package com.latticeengines.app.exposed.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "metadata", description = "Common REST resource for attributes")
@RestController
@RequestMapping("/datacollection/attributes")
public class DataLakeAttributeResource {

    private static final Logger log = LoggerFactory.getLogger(DataLakeAttributeResource.class);

    private final DataLakeService dataLakeService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    public DataLakeAttributeResource(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }

    @GetMapping("/count")
    @ResponseBody
    @ApiOperation(value = "Get number of attributes")
    public long getAttributesCount() {
        try {
            DataCollectionStatus status = dataCollectionProxy
                    .getOrCreateDataCollectionStatus(MultiTenantContext.getCustomerSpace().toString(), null);
            if (status.getAccountCount() == 0) {
                return 0L;
            } else {
                return dataLakeService.getAttributesCount();
            }
        } catch (Exception e) {
            log.error("Failed to get attribute count", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes")
    public List<ColumnMetadata> getAttributes( //
                                                @ApiParam(value = "Offset for pagination of matching attributes")//
                                                @RequestParam(value = "offset", required = false)//
                                                        Integer offset, //
                                                @ApiParam(value = "Maximum number of matching attributes in page")//
                                                @RequestParam(value = "max", required = false)//
                                                        Integer max) {
        try {
            return dataLakeService.getAttributes(offset, max);
        } catch (Exception e) {
            log.error("Failed to get attributes", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

}
