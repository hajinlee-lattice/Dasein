package com.latticeengines.app.exposed.controller;

import static com.latticeengines.domain.exposed.exception.LedpCode.LEDP_36002;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "metadata", description = "Common REST resource for attributes")
@RestController
@RequestMapping("/datacollection/attributes")
public class DataLakeAttributeResource {

    private final DataLakeService dataLakeService;

    @Inject
    public DataLakeAttributeResource(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }

    @RequestMapping(value = "/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get number of attributes")
    public long getAttributesCount() {
        try {
            return dataLakeService.getAttributesCount();
        } catch (Exception e) {
            throw new LedpException(LEDP_36002);
        }
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
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
            throw new LedpException(LEDP_36002);
        }
    }

    @RequestMapping(value = "/predefined/{groupName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes in a group")
    public List<ColumnMetadata> getAttributesInPredefinedGroup(@PathVariable ColumnSelection.Predefined groupName) {
        try {
            return dataLakeService.getAttributesInPredefinedGroup(groupName);
        } catch (Exception e) {
            throw new LedpException(LEDP_36002);
        }
    }

}
