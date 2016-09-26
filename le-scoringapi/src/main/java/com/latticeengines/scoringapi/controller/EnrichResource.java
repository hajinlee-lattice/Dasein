package com.latticeengines.scoringapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.scoringinternalapi.controller.BaseEnrich;

@Api(value = "enrich", description = "REST resource for retrieving enrichment values")
@RestController
@RequestMapping(value = "/enrich")
public class EnrichResource extends BaseEnrich {

    @RequestMapping(value = "/record/{uuid}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get enrichment values for one record")
    public EnrichResponse getLeadEnrichmentValues(HttpServletRequest request, @PathVariable String uuid,
            @RequestBody EnrichRequest enrichRequest) {
        Pair<String, String> unpacked = UuidUtils.unpackPairUuid(uuid);
        String tenantId = unpacked.getKey();
        String credentialId = unpacked.getValue();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        EnrichResponse response = enrichRecord(request, enrichRequest, customerSpace, credentialId);
        String requestId = "";
        Object identifier = request.getAttribute(RequestLogInterceptor.IDENTIFIER_KEY);
        if (!StringUtils.objectIsNullOrEmptyString(identifier)) {
            requestId = String.valueOf(identifier);
        }
        response.setRequestId(requestId);
        return response;
    }

}
