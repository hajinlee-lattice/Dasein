package com.latticeengines.remote.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.Metadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.remote.exposed.exception.MetadataValidationException;
import com.latticeengines.remote.exposed.service.MetadataValidationResult;

public class MetadataValidationServiceTestNG extends RemoteFunctionalTestNGBase {

    @Autowired
    private MetadataValidationServiceImpl metadataValidationService;

    @Test(groups = { "functional" })
    public void testGenerateMetadataValidationResult() throws IOException, ParseException {
        URL metadataUrl = ClassLoader.getSystemResource("incorrect-metadata.json");
        String metadataContents = FileUtils.readFileToString(new File(metadataUrl.getPath()));
        Metadata metadataObj = JsonUtils.deserialize(metadataContents, Metadata.class);
        List<AttributeMetadata> attributeMetadata = metadataObj.getAttributeMetadata();

        MetadataValidationResult result = metadataValidationService.generateMetadataValidationResult(attributeMetadata);
        assertTrue(result.getApprovedUsageAnnotationErrors().size() == 2);
        assertTrue(result.getTagsAnnotationErrors().size() == 3);
        assertTrue(result.getCategoryAnnotationErrors().size() == 2);
        assertTrue(result.getDisplayNameAnnotationErrors().size() == 2);
        assertTrue(result.getStatisticalTypeAnnotationErrors().size() == 2);
    }

    @Test(groups = { "functional" })
    public void testValidateThrowsException() throws IOException, ParseException {
        URL metadataUrl = ClassLoader.getSystemResource("incorrect-metadata.json");
        String metadataContents = FileUtils.readFileToString(new File(metadataUrl.getPath()));
        String jsonString = null;
        try {
            metadataValidationService.validate(metadataContents);
        } catch (Exception e) {
            assertTrue(e instanceof MetadataValidationException);
            jsonString = e.getMessage();
        }
        // Assert validationValidationResult is in json format
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);
        JSONArray approvedUsageError = (JSONArray) jsonObject.get("ApprovedUsageAnnotationErrors");
        assertEquals(approvedUsageError.size(), 2);
        JSONArray tagsError = (JSONArray) jsonObject.get("TagsAnnotationErrors");
        assertEquals(tagsError.size(), 3);
        JSONArray categoryError = (JSONArray) jsonObject.get("CategoryAnnotationErrors");
        assertEquals(categoryError.size(), 2);
        JSONArray displayNameError = (JSONArray) jsonObject.get("DisplayNameAnnotationErrors");
        assertEquals(displayNameError.size(), 2);
        JSONArray statTypeError = (JSONArray) jsonObject.get("StatisticalTypeAnnotationErrors");
        assertEquals(statTypeError.size(), 2);
    }

    @Test(groups = { "functional" })
    public void testValidateDoNotThrowsException() throws IOException, ParseException {
        URL metadataUrl = ClassLoader.getSystemResource("correct-metadata.json");
        String metadataContents = FileUtils.readFileToString(new File(metadataUrl.getPath()));
        try {
            metadataValidationService.validate(metadataContents);
        } catch (MetadataValidationException e) {
            e.printStackTrace();
        }
        assertTrue(true, "Should not catch any exception");
    }
}
