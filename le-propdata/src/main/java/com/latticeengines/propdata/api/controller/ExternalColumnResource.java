package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.network.exposed.propdata.ExternalColumnInterface;
import com.latticeengines.propdata.core.service.ExternalColumnService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "externalcolumn", description = "REST resource for external columns")
@RestController
@RequestMapping("/externalcolumn")
public class ExternalColumnResource implements ExternalColumnInterface{
	
	@Autowired
	private ExternalColumnService externalColumnService;
	
	@RequestMapping(value = "/leadenrichment", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Return external columns of lead enrichment.")
    @Override
    public List<ExternalColumn> getLeadEnrichment() {
		return externalColumnService.getLeadEnrichment();
	}
}
