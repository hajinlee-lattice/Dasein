package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.Constants;

public class VdbMetadataResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Integer maxUpdatesCount = 5;

    private List<VdbMetadataField> originalFields;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        switchToSuperAdmin();
        originalFields = getFields();
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        if (originalFields != null && originalFields.size() > 0) {
            switchToSuperAdmin();
            String url = getRestAPIHostPort() + "/pls/vdbmetadata/fields";
            if (originalFields.size() > maxUpdatesCount) {
                restTemplate.put(url, originalFields.subList(0, maxUpdatesCount - 1), SimpleBooleanResponse.class);
            } else {
                restTemplate.put(url, originalFields, SimpleBooleanResponse.class);
            }
        }
    }

    @Test(groups = "deployment", enabled = false)
    public void testGetFields() {
        switchToSuperAdmin();
        assertGetFieldsSuccess();

        switchToInternalAdmin();
        assertGetFieldsSuccess();

        switchToInternalUser();
        assertGetFieldsGet403();

        switchToExternalAdmin();
        assertGetFieldsGet403();

        switchToExternalUser();
        assertGetFieldsGet403();
    }

    @Test(groups = "deployment", enabled = false)
    public void testUpdateField() {
        switchToSuperAdmin();
        VdbMetadataField fieldToUpdate = getFieldToUpdate();
        assertUpdateFieldSuccess(fieldToUpdate);

        switchToInternalAdmin();
        fieldToUpdate = getFieldToUpdate();
        assertUpdateFieldSuccess(fieldToUpdate);

        switchToInternalUser();
        assertUpdateFieldGet403(fieldToUpdate);

        switchToExternalAdmin();
        assertUpdateFieldGet403(fieldToUpdate);

        switchToExternalUser();
        assertUpdateFieldGet403(fieldToUpdate);
    }

    @Test(groups = "deployment", enabled = false)
    public void testUpdateFields() {
        switchToSuperAdmin();
        List<VdbMetadataField> fieldsToUpdate = getFieldsToUpdate();
        assertUpdateFieldsSuccess(fieldsToUpdate);

        switchToInternalAdmin();
        fieldsToUpdate = getFieldsToUpdate();
        assertUpdateFieldsSuccess(fieldsToUpdate);

        switchToInternalUser();
        assertUpdateFieldsGet403(fieldsToUpdate);

        switchToExternalAdmin();
        assertUpdateFieldsGet403(fieldsToUpdate);

        switchToExternalUser();
        assertUpdateFieldsGet403(fieldsToUpdate);
    }

    private void assertGetFieldsSuccess() {
        getFields();
    }

    private void assertGetFieldsGet403() {
        boolean exception = false;
        try {
            getFields();
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    private VdbMetadataField getFieldToUpdate() {
        VdbMetadataField field = (VdbMetadataField)originalFields.get(0).clone();
        String displayName = "DisplayName_DeploymentTest_0";
        field.setDisplayName(displayName);
        return field;
    }

    private void assertUpdateFieldSuccess(VdbMetadataField fieldToUpdate) {
        String url = getRestAPIHostPort() + "/pls/vdbmetadata/fields/" + fieldToUpdate.getColumnName();
        restTemplate.put(url, fieldToUpdate, SimpleBooleanResponse.class);

        List<VdbMetadataField> fields = getFields();
        VdbMetadataField fieldUpdated = getField(fields, fieldToUpdate.getColumnName());
        Assert.assertTrue(fieldToUpdate.equals(fieldUpdated));
    }

    private void assertUpdateFieldGet403(VdbMetadataField fieldToUpdate) {
        boolean exception = false;
        try {
            String url = getRestAPIHostPort() + "/pls/vdbmetadata/fields/" + fieldToUpdate.getColumnName();
            restTemplate.put(url, fieldToUpdate, SimpleBooleanResponse.class);
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    private List<VdbMetadataField> getFieldsToUpdate() {
        List<VdbMetadataField> fieldsToUpdate = new ArrayList<VdbMetadataField>();
        Random random = new Random();
        Integer maxCount = originalFields.size() > maxUpdatesCount ? maxUpdatesCount : originalFields.size();
        for (int i = 0; i < maxCount; i++) {
            VdbMetadataField field = (VdbMetadataField)originalFields.get(i).clone();
            field.setDisplayName("DisplayName_DeploymentTest_" + random.nextInt(1000));
            fieldsToUpdate.add(field);
        }
        return fieldsToUpdate;
    }

    private void assertUpdateFieldsSuccess(List<VdbMetadataField> fieldsToUpdate) {
        String url = getRestAPIHostPort() + "/pls/vdbmetadata/fields";
        restTemplate.put(url, fieldsToUpdate, SimpleBooleanResponse.class);

        List<VdbMetadataField> fields = getFields();
        for (VdbMetadataField field : fieldsToUpdate) {
            VdbMetadataField fieldUpdated = getField(fields, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }
    }

    private void assertUpdateFieldsGet403(List<VdbMetadataField> fields) {
        boolean exception = false;
        try {
            String url = getRestAPIHostPort() + "/pls/vdbmetadata/fields";
            restTemplate.put(url, fields, SimpleBooleanResponse.class);
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<VdbMetadataField> getFields() {
        String url = getRestAPIHostPort() + "/pls/vdbmetadata/fields";
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        headers.add(Constants.INTERNAL_SERVICE_HEADERNAME, Constants.INTERNAL_SERVICE_HEADERVALUE);
        HttpEntity<String> request = new HttpEntity<>("", headers);
        ParameterizedTypeReference responseType = new ParameterizedTypeReference<ResponseDocument<List<VdbMetadataField>>>() {};
        ResponseEntity<ResponseDocument<List<VdbMetadataField>>> responseEntity = restTemplate.exchange(
                url, HttpMethod.GET, request, responseType);
        ResponseDocument<List<VdbMetadataField>> response = responseEntity.getBody();

        Assert.assertNotNull(response);
        Assert.assertTrue(response.isSuccess());
        List<VdbMetadataField> fields = response.getResult();
        Assert.assertTrue(fields != null && fields.size() > 0);

        return fields;
    }

    protected VdbMetadataField getField(List<VdbMetadataField> fields, String columnName) {
        for (VdbMetadataField field : fields) {
            if (columnName.equals(field.getColumnName())) {
                return field;
            }
        }

        return null;
    }

}
