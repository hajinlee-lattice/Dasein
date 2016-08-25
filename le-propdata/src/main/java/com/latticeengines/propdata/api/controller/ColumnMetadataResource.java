package com.latticeengines.propdata.api.controller;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ColumnSelectionService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "columnmetadata", description = "REST resource for column metadata")
@RestController
@RequestMapping("/metadata")
public class ColumnMetadataResource implements ColumnMetadataInterface {

    @Resource(name = "columnMetadataServiceDispatch")
    private ColumnMetadataService columnMetadataService;

    @Autowired
    @Qualifier("columnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @RequestMapping(value = "/predefined/{selectName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Available choices for selectName are LeadEnrichment, DerivedColumns and Model (case-sensitive)")
    @Override
    public List<ColumnMetadata> columnSelection(@PathVariable Predefined selectName,
            @RequestParam(value = "datacloudversion", required = false) String dataCloudVersion) {
        try {
            return columnMetadataService.fromPredefinedSelection(selectName, dataCloudVersion);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25006, e, new String[] { selectName.getName() });
        }
    }

    @RequestMapping(value = "/predefined/{selectName}/currentversion", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get current version of predefine selection")
    @Override
    public String selectionCurrentVersion(@PathVariable Predefined selectName) {
        try {
            return columnSelectionService.getCurrentVersion(selectName);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25020, e, new String[] { selectName.getName() });
        }
    }

}
