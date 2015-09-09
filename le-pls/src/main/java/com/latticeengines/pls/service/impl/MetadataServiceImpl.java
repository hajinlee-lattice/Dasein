package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MetadataField;
import com.latticeengines.pls.service.MetadataConstants;
import com.latticeengines.pls.service.MetadataService;
import com.latticeengines.remote.service.impl.DataLoaderServiceImpl;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {

    @Autowired
    private DataLoaderServiceImpl dataLoaderService;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<MetadataField> getMetadataFields(String tenantName, String dlUrl) {
        try {
            // TODO: this is a mock up completion
            List<MetadataField> fields = new ArrayList<MetadataField>();
            String[] queries = new String[] { "Q_PLS_Modeling" };
            for (String query : queries) {
                Map<String, String> paramerters = new HashMap<>();
                paramerters.put("tenantName", tenantName);
                paramerters.put("queryName", query);

                String response = dataLoaderService.callDLRestService(dlUrl, "/GetQueryMetadataColumns", paramerters);
                JsonNode json = objectMapper.readTree(response);
                if (json.get("Status").asInt() != 3) { // STATUS_SUCCESS = 3
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
                    //throw new IllegalStateException("Returned status from DL is not SUCCESS.");
                }
                for (JsonNode kvpair: json.get("Metadata")) {
                    String approvedUsage;
                    JsonNode approvedUsageNode = kvpair.get("ApprovedUsage");
                    if (approvedUsageNode.isArray()) {
                        approvedUsage = getNodeText(approvedUsageNode.get(approvedUsageNode.size() - 1));
                    } else {
                        approvedUsage = getNodeText(approvedUsageNode);
                    }
                    MetadataField field = createField(
                        getNodeText(kvpair.get("ColumnName")),
                        getNodeText(kvpair.get("DataSource")),
                        getNodeText(kvpair.get("Object")),
                        getNodeText(kvpair.get("Category")),
                        getNodeText(kvpair.get("DisplayName")),
                        approvedUsage,
                        getNodeText(kvpair.get("Tags")),
                        getNodeText(kvpair.get("FundamentalType")),
                        getNodeText(kvpair.get("DisplayDiscretizationStrategy")),
                        getNodeText(kvpair.get("StatisticalType")),
                        getNodeText(kvpair.get("Description"))
                    );
                    fields.add(field);
                }
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