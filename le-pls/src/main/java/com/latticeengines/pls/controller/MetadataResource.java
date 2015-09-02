package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.MetadataField;
import com.latticeengines.pls.service.impl.TenantConfigServiceImpl;
import com.latticeengines.security.exposed.service.SessionService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata")
@RestController
@RequestMapping(value = "/metadata")
public class MetadataResource {

    @Autowired
    private SessionService sessionService;

    @Autowired
    private TenantConfigServiceImpl tenantConfigService;

    @RequestMapping(value = "/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of metadata fields")
    public ResponseDocument<List<MetadataField>> getFields(HttpServletRequest request) {

        ResponseDocument<List<MetadataField>> response = new ResponseDocument<>();
        try
        {
            List<MetadataField> fields = new ArrayList<MetadataField>();
            for (Integer i = 0; i < 60; i++) {
                String idx = i > 0 ? i.toString() : "";
                MetadataField field = createField("ID" + idx, "Marketo", "Lead", "Lead Info", "ID", "None", "Internal", "Test", null, "Test", null);
                fields.add(field);
                field = createField("Email" + idx, "Marketo", "Lead", "Lead Info", "Email Address", "ModelOnly", "Internal", "Test", null, "Test", null);
                fields.add(field);
                field = createField("Phone" + idx, "Marketo", "Account", "Firmographics", "Phone Number", "ModelOnly", "Internal", "Test", null, "Test", null);
                fields.add(field);
                field = createField("Address" + idx, "Salesforce", "Lead", "Firmographics", "Address", "", "Internal", "Test", null, "Test", null);
                fields.add(field);
                field = createField("Employees" + idx, "Data Cloud", null, "Lead Info", "Address", "", "", null, null, null, null);
                fields.add(field);
            }

            response.setSuccess(true);
            response.setResult(fields);
        } catch (Exception ex) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(ex.getMessage()));
        }

        return response;
    }

    private MetadataField createField(String columnName, String source, String object, String category,
            String displayName, String approvedUsage, String tags, String fundamentalType,
            String displayDiscretization, String statisticalType, String description) {
        MetadataField field = new MetadataField();
        field.setColumnName(columnName);
        field.setSource(source);
        field.setObject(object);
        field.setCategory(category);
        field.setDisplayName(displayName);
        field.setApprovedUsage(approvedUsage);
        field.setTags(tags);
        field.setFundamentalType(fundamentalType);
        field.setDisplayDiscretization(displayDiscretization);
        field.setStatisticalType(statisticalType);
        field.setDescription(description);
        return field;
    }

}
