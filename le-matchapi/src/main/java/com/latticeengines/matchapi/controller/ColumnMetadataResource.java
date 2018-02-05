package com.latticeengines.matchapi.controller;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.exposed.service.BeanDispatcher;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
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

    @GetMapping(value = "/predefined/{selectName}")
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

    @GetMapping(value = "/versions")
    @ResponseBody
    @ApiOperation(value = "Get all known data cloud versions")
    public List<DataCloudVersion> getVersions() {
        return dataCloudVersionService.allVerions();
    }

    @GetMapping(value = "/versions/latest")
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

    @GetMapping(value = "/")
    @ResponseBody
    @ApiOperation(value = "Get all columns belong to a data cloud version")
    public List<ColumnMetadata> getAllColumns(
            @RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        ColumnMetadataService columnMetadataService = beanDispatcher
                .getColumnMetadataService(dataCloudVersion);
        return columnMetadataService.findAll(dataCloudVersion);
    }

    @ApiIgnore
    @GetMapping(value = "/statscube")
    @ApiOperation(value = "Get enrichment stats cube.")
    public StatsCube getStatsCube(@RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        if (StringUtils.isBlank(dataCloudVersion)){
            dataCloudVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        }
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
        return columnMetadataService.getStatsCube(dataCloudVersion);
    }

    @ApiIgnore
    @GetMapping(value = "/topn")
    @ApiOperation(value = "Get enrichment topn tree.")
    public TopNTree getTopNTree(@RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        if (StringUtils.isBlank(dataCloudVersion)){
            dataCloudVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        }
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
        return columnMetadataService.getTopNTree(dataCloudVersion);
    }

}
