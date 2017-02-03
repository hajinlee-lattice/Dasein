package com.latticeengines.ulysses.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

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
    @ApiOperation(value = "Validation Expression with different business rules,"
            + " to invoke Scoring API / Company Profile API / enforcing " + "Model Mapping", response = String.class)
    public void getSimplifiedValidationExpression(HttpServletResponse response) {

        String result = "((Website||Email||CompanyName)&&(Id))";
        writePlainTextToResponse(response, result);
    }

    private void writePlainTextToResponse(HttpServletResponse response, String result) {
        try {
            OutputStream os = response.getOutputStream();
            response.setContentType("text/plain");
            os.write(result.getBytes());
            os.flush();
            os.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
