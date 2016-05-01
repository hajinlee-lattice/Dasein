package com.latticeengines.propdata.api.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.network.exposed.propdata.IngestionInterface;
import com.latticeengines.propdata.engine.ingestion.service.IngestionService;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ingestion", description = "REST resource for source ingestion")
@RestController
@RequestMapping("/ingestions")
public class IngestionResource extends InternalResourceBase implements IngestionInterface {

    @Autowired
    private IngestionService ingestionService;

    @Override
    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @Override
    public List<IngestionProgress> scan(String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Scan and trigger all ingestions that can proceed.")
    public List<IngestionProgress> scan(
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            return ingestionService.scan(hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25017, e);
        }
    }

    @RequestMapping(value = "/internal/{ingestionName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Forcefully start an ingestion to download designated file. "
            + "If an ingestion for same file source is going on, skip the operation")
    public IngestionProgress ingestInternal(@PathVariable String ingestionName,
            @RequestBody IngestionRequest ingestionRequest,
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            checkIngestionRequest(ingestionRequest);
            return ingestionService.ingestInternal(ingestionName, ingestionRequest, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25017, e);
        }
    }

    private void checkIngestionRequest(IngestionRequest ingestionRequest) {
        if (StringUtils.isEmpty(ingestionRequest.getFileName())) {
            throw new IllegalArgumentException("Please provide file name in ingestion request");
        }
        if (StringUtils.isEmpty(ingestionRequest.getSubmitter())) {
            throw new IllegalArgumentException("Please provide submitter in ingestion request");
        }
    }
}
