package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MetadataField;
import com.latticeengines.pls.service.MetadataConstants;
import com.latticeengines.pls.service.MetadataService;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {

    @Override
    public List<MetadataField> getMetadataFields(String tenantName, String dlUrl) {
        try {
            // TODO: this is a mock up completion
            List<MetadataField> fields = new ArrayList<>();
            for (Integer i = 0; i < 60; i++) {
                String idx = i > 0 ? i.toString() : "";
                MetadataField field = createField("ID" + idx, "Marketo", "Lead", "Lead Information", "ID", "None", "Internal", "URI", null, "ratio", null);
                fields.add(field);
                field = createField("Email" + idx, "Marketo", "Lead", "Lead Information", "Email Address", "Model", "Internal", "URI", null, "ratio", null);
                fields.add(field);
                field = createField("Phone" + idx, "Marketo", "Account", "Marketing Activity", "Phone Number", "Model", "Internal", "URI", null, "ratio", null);
                fields.add(field);
                field = createField("Address" + idx, "Salesforce", "Lead", "Marketing Activity", "Address", "", "Internal", "URI", null, "ratio", null);
                fields.add(field);
                field = createField("Employees" + idx, "PD_Alexa_Source_Import", null, "Lead Information", "Address", "", "", null, null, null, null);
                fields.add(field);
            }
            return fields;
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18046, ex, new String[] { ex.getMessage() });
        }
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
        String sourceToDisplay = getSourceToDisplay(source);
        field.setSourceToDisplay(sourceToDisplay);
        return field;
    }

    private String getNodeText(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        } else {
            return node.asText();
        }
    }

    @Override
    public String getSourceToDisplay(String source) {
        if (source == null) {
            return "";
        }

        boolean exist = MetadataConstants.SOURCE_MAPPING.containsKey(source);
        if (exist) {
            return MetadataConstants.SOURCE_MAPPING.get(source);
        } else {
            return source;
        }
    }

    @Override
    public void UpdateField(String tenantName, String dlUrl, MetadataField field) {
        try {
            // TODO Auto-generated method stub
            Thread.sleep(1000);

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18047, ex, new String[] { ex.getMessage() });
        }
    }

    @Override
    public void UpdateFields(String tenantName, String dlUrl, List<MetadataField> fields) {
        try {
            // TODO Auto-generated method stub
            Thread.sleep(1000);

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18048, ex, new String[] { ex.getMessage() });
        }
    }

}