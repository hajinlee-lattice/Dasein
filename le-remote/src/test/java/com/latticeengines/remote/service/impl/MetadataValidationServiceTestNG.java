package com.latticeengines.remote.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.Metadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.remote.exposed.exception.MetadataValidationException;
import com.latticeengines.remote.exposed.service.MetadataValidationResult;

public class MetadataValidationServiceTestNG extends RemoteFunctionalTestNGBase {

    @Autowired
    private MetadataValidationServiceImpl metadataValidationService;

    @Test(groups = { "functional" })
    public void testGenerateMetadataValidationResult() throws IOException {
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
    public void testValidateThrowsException() throws IOException {
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
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonObject = jsonParser.readTree(jsonString);
        ArrayNode approvedUsageError = (ArrayNode) jsonObject.get("ApprovedUsageAnnotationErrors");
        assertEquals(approvedUsageError.size(), 2);
        ArrayNode tagsError = (ArrayNode) jsonObject.get("TagsAnnotationErrors");
        assertEquals(tagsError.size(), 3);
        ArrayNode categoryError = (ArrayNode) jsonObject.get("CategoryAnnotationErrors");
        assertEquals(categoryError.size(), 2);
        ArrayNode displayNameError = (ArrayNode) jsonObject.get("DisplayNameAnnotationErrors");
        assertEquals(displayNameError.size(), 2);
        ArrayNode statTypeError = (ArrayNode) jsonObject.get("StatisticalTypeAnnotationErrors");
        assertEquals(statTypeError.size(), 2);
    }

    @Test(groups = { "functional" })
    public void testValidateDoNotThrowsException() throws IOException {
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
