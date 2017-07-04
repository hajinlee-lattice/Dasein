package com.latticeengines.matchapi.controller;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.exposed.service.BeanDispatcher;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "columnmetadata", description = "REST resource for column metadata")
@RestController
@RequestMapping("/metadata")
public class ColumnMetadataResource {

    @Autowired
    private BeanDispatcher beanDispatcher;

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    @RequestMapping(value = "/predefined/{selectName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Available choices for selectName are LeadEnrichment, DerivedColumns and Model (case-sensitive)")
    public List<ColumnMetadata> columnSelection(@PathVariable Predefined selectName,
            @RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        try {
            ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
            return columnMetadataService.fromPredefinedSelection(selectName, dataCloudVersion);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25006, e, new String[] { selectName.getName() });
        }
    }

    @RequestMapping(value = "/versions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all known data cloud versions")
    public List<DataCloudVersion> getVersions() {
        return dataCloudVersionService.allVerions();
    }

    @RequestMapping(value = "/versions/latest", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get latest approved data cloud version. If query parameter compatibleto is provided. "
            + "Will return latest approved version under the same major version.")
    public DataCloudVersion latestVersion(
            @RequestParam(value = "compatibleto", required = false) String compatibleToVersion) {
        if (StringUtils.isEmpty(compatibleToVersion)) {
            compatibleToVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        }
        return dataCloudVersionService.latestApprovedForMajorVersion(compatibleToVersion);
    }

    @RequestMapping(value = "/", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all columns belong to a data cloud version")
    public List<ColumnMetadata> getAllColumns(
            @RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        ColumnMetadataService columnMetadataService = beanDispatcher
                .getColumnMetadataService(dataCloudVersion);
        return columnMetadataService.findAll(dataCloudVersion);
    }

    @ApiIgnore
    @RequestMapping(value = "/", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Update the columns for a specific cloud version")
    public void updateColumnsForDataCloudVersion(
            @RequestParam(value = "datacloudversion", required = true) String dataCloudVersion,
            @RequestBody List<ColumnMetadata> columnMetadatas) {
        ColumnMetadataService columnMetadataService = beanDispatcher
                .getColumnMetadataService(dataCloudVersion);
        columnMetadataService.updateColumnMetadatas(dataCloudVersion, columnMetadatas);
    }

    @ApiIgnore
    @RequestMapping(value = "/attrrepo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get attribute repository.")
    public AttributeRepository getAttrRepoAtVersion(
            @RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        if (StringUtils.isBlank(dataCloudVersion)){
            dataCloudVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        }
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
        return columnMetadataService.getAttrRepo(dataCloudVersion);
    }

}
