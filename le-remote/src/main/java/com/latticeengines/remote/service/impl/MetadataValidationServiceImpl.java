package com.latticeengines.remote.service.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.expection.AnnotationValidationError;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.modeling.Metadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.remote.exposed.exception.MetadataValidationException;
import com.latticeengines.remote.exposed.service.MetadataValidationResult;
import com.latticeengines.remote.exposed.service.MetadataValidationService;

@Component("metadataValidationService")
public class MetadataValidationServiceImpl implements MetadataValidationService {
    private static final Log log = LogFactory.getLog(MetadataValidationServiceImpl.class);

    @Autowired
    private BeanValidationServiceImpl beanValidationServiceImpl;

    public void validate(String metadata) throws MetadataValidationException {

        Metadata metadataObj = JsonUtils.deserialize(metadata, Metadata.class);
        List<AttributeMetadata> attributeMetadata = metadataObj.getAttributeMetadata();
        MetadataValidationResult result = generateMetadataValidationResult(attributeMetadata);
        if (!result.isCorrect()) {
            throw new MetadataValidationException(result.toString());
        }
        return;
    }

    @VisibleForTesting
    MetadataValidationResult generateMetadataValidationResult(List<AttributeMetadata> attributeMetadata) {

        List<Map.Entry<String, AnnotationValidationError>> approvedUsageAnnotationErrors = new ArrayList<Map.Entry<String, AnnotationValidationError>>();
        List<Map.Entry<String, AnnotationValidationError>> tagsAnnotationErrors = new ArrayList<Map.Entry<String, AnnotationValidationError>>();
        List<Map.Entry<String, AnnotationValidationError>> categoryAnnotationErrors = new ArrayList<Map.Entry<String, AnnotationValidationError>>();
        List<Map.Entry<String, AnnotationValidationError>> displayNameAnnotationErrors = new ArrayList<Map.Entry<String, AnnotationValidationError>>();
        List<Map.Entry<String, AnnotationValidationError>> statisticalTypeAnnotationErrors = new ArrayList<Map.Entry<String, AnnotationValidationError>>();

        for (AttributeMetadata am : attributeMetadata) {
            Set<AnnotationValidationError> errorSet = beanValidationServiceImpl.validate(am);
            String columnName = am.getColumnName(); // columnName should be the
                                                    // primary key for
                                                    // AttributeMetadata
            assert columnName != null;
            for (AnnotationValidationError error : errorSet) {
                Map.Entry<String, AnnotationValidationError> errorMap = new AbstractMap.SimpleEntry<String, AnnotationValidationError>(
                        columnName, error);
                String fieldName = error.getFieldName();
                switch (fieldName) {
                case "approvedUsage":
                    approvedUsageAnnotationErrors.add(errorMap);
                    break;
                case "tags":
                    tagsAnnotationErrors.add(errorMap);
                    break;
                case "extensions":
                    categoryAnnotationErrors.add(errorMap);
                    break;
                case "displayName":
                    displayNameAnnotationErrors.add(errorMap);
                    break;
                case "statisticalType":
                    statisticalTypeAnnotationErrors.add(errorMap);
                    break;
                default:
                    log.info(String.format("%s does not belong to modeling or display validation error",
                            error.getAnnotationName()));
                    break;
                }
            }
        }
        MetadataValidationResult result = new MetadataValidationResult.Builder()
                .approvedUsageAnnotationErrors(approvedUsageAnnotationErrors)
                .categoryAnnotationErrors(categoryAnnotationErrors)
                .displayNameAnnotationErrors(displayNameAnnotationErrors)
                .statisticalTypeAnnotationErrors(statisticalTypeAnnotationErrors)
                .tagsAnnotationErrors(tagsAnnotationErrors).build();

        return result;
    }
}
