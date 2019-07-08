package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ExportFieldMetadataService;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.impl.ExportFieldMetadataServiceBase;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "export-field-metadata", description = "Rest resource for export field metadata")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/export-field-metadata")
public class ExportFieldMetadataResource {

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get export field metadata")
    public List<ColumnMetadata> getExportFieldMetadata(@PathVariable String customerSpace,
            @RequestParam(value = "channelId", required = true) String playChannelId) {
        PlayLaunchChannel channel = playLaunchChannelService.findById(playChannelId);
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(customerSpace, channel);
        return columnMetadata;
    }

}
