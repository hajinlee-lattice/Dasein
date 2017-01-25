package com.latticeengines.ulysses.controller;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.ulysses.service.AttributeService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "attributes", description = "REST resource for attribute configurations")
@RestController
@RequestMapping("/attributes")
public class AttributeResource {
    private static final Log log = LogFactory.getLog(AttributeResource.class);

    @Autowired
    private AttributeService attributeService;

    @RequestMapping(value = "/primary", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Provides all matching attrinutes that are supported for ModelMapping or Scoring or Company Lookup API")
    public List<PrimaryField> getPrimaryAttributes() {
        return attributeService.getPrimaryFields();
    }

    @RequestMapping(value = "/primary/validation-expression", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validation Expression with different business rules, to invoke Scoring API / Company Profile API / enforcing Model Mapping ")
    public String getSimplifiedValidationExpression() {
        return "((Website||Email||CompanyName)&&(Id))";
    }
}
