package com.latticeengines.app.exposed.controller.datacollection;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "Common REST resource for attributes")
@RestController
@RequestMapping("/datacollection/attributes")
public class CommonAttributeResource {

    @Autowired
    private DataLakeService dataLakeService;

    @RequestMapping(value = "/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get number of attributes")
    private int getAttributesCount() {
        return dataLakeService.getAttributes(null, null).size();
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes")
    private List<ColumnMetadata> getAttributes( //
                                                @ApiParam(value = "Offset for pagination of matching attributes")//
                                                @RequestParam(value = "offset", required = false)//
                                                        Integer offset, //
                                                @ApiParam(value = "Maximum number of matching attributes in page")//
                                                @RequestParam(value = "max", required = false)//
                                                        Integer max) {
        return dataLakeService.getAttributes(offset, max);
    }

    @RequestMapping(value = "/{entity}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes for a business entity")
    private List<ColumnMetadata> getAttributesInEntity(@PathVariable BusinessEntity entity) {
        return dataLakeService.getAttributesInEntity(entity);
    }

    @RequestMapping(value = "/{entity}/demo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes for a business entity")
    private List<ColumnMetadata> getDemoAttributesInEntity(@PathVariable BusinessEntity entity) {
        return dataLakeService.getAttributesInEntity(entity);
    }

}
