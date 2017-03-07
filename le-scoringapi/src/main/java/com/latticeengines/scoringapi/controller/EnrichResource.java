package com.latticeengines.scoringapi.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.scoringinternalapi.controller.BaseEnrich;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "enrich", description = "REST resource for retrieving enrichment values")
@RestController
@RequestMapping(value = "/enrich")
public class EnrichResource extends BaseEnrich {

    @RequestMapping(value = "/record/{uuid}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get enrichment values for one record")
    public void getLeadEnrichmentValues(HttpServletRequest request, @PathVariable String uuid,
            @RequestBody EnrichRequest enrichRequest, HttpServletResponse response) {
        Pair<String, String> unpacked = UuidUtils.unpackPairUuid(uuid);
        String tenantId = unpacked.getKey();
        String credentialId = unpacked.getValue();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        EnrichResponse enrichResponse = enrichRecord(request, enrichRequest, customerSpace, credentialId);
        String requestId = RequestLogInterceptor.getRequestIdentifierId(request);
        enrichResponse.setRequestId(requestId);
        try {
            GzipUtils.writeToGzipStream(response, enrichResponse);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31023, e);
        }
    }
}
