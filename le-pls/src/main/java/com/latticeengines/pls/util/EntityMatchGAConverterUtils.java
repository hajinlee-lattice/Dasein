package com.latticeengines.pls.util;

import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public final class EntityMatchGAConverterUtils {

    protected EntityMatchGAConverterUtils() {
        throw new UnsupportedOperationException();
    }

    public static void convertGuessingMappings(boolean enableEntityMatch, boolean enableEntityMatchGA,
                                               FieldMappingDocument fieldMappingDocument) {
        if (!enableEntityMatchGA) {
            return;
        }
        if (enableEntityMatch) {
            return;
        }
        if (fieldMappingDocument == null || CollectionUtils.isEmpty(fieldMappingDocument.getFieldMappings())) {
            return;
        }
        boolean containsCustomerAccountId = false;
        boolean containsAccountId = false;
        boolean containsCustomerContactId = false;
        boolean containsContactId = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                containsAccountId = true;
            }
            if (InterfaceName.ContactId.name().equals(fieldMapping.getMappedField())) {
                containsContactId = true;
            }
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                containsCustomerAccountId = true;
            }
            if (InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField())) {
                containsCustomerContactId = true;
            }
        }
        if (containsAccountId && containsCustomerAccountId) {
            fieldMappingDocument.getFieldMappings()
                    .removeIf(fieldMapping -> InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField()));
        } else if (containsCustomerAccountId) {
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setMappedField(InterfaceName.AccountId.name());
                }
            }
        }
        if (containsContactId && containsCustomerContactId) {
            fieldMappingDocument.getFieldMappings()
                    .removeIf(fieldMapping -> InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()));
        } else if (containsCustomerContactId) {
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setMappedField(InterfaceName.ContactId.name());
                }
            }
        }
    }

    public static void convertSavingMappings(boolean enableEntityMatch, boolean enableEntityMatchGA,
                                             FieldMappingDocument fieldMappingDocument, S3ImportSystem defaultSystem) {
        if (!enableEntityMatchGA) {
            return;
        }
        if (enableEntityMatch) {
            return;
        }
        if (fieldMappingDocument == null || CollectionUtils.isEmpty(fieldMappingDocument.getFieldMappings())) {
            return;
        }
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setMappedField(InterfaceName.CustomerAccountId.name());
            } else if (InterfaceName.ContactId.name().equals(fieldMapping.getMappedField())) {
                fieldMapping.setMappedField(InterfaceName.CustomerContactId.name());
            }
        }
        // sync system id mapping with customer Id mapping.
        if (defaultSystem != null) {
            if (StringUtils.isNotEmpty(defaultSystem.getAccountSystemId())) {
                Optional<FieldMapping> customerAccountIdMapping = fieldMappingDocument.getFieldMappings().stream()
                        .filter(fieldMapping -> InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField()))
                        .findAny();
                if (customerAccountIdMapping.isPresent()) {
                    for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                        if (defaultSystem.getAccountSystemId().equals(fieldMapping.getMappedField())) {
                            fieldMapping.setUserField(customerAccountIdMapping.get().getUserField());
                        }
                    }
                }
            }
            if (StringUtils.isNotEmpty(defaultSystem.getContactSystemId())) {
                Optional<FieldMapping> customerContactIdMapping = fieldMappingDocument.getFieldMappings().stream()
                        .filter(fieldMapping -> InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()))
                        .findAny();
                if (customerContactIdMapping.isPresent()) {
                    for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                        if (defaultSystem.getContactSystemId().equals(fieldMapping.getMappedField())) {
                            fieldMapping.setUserField(customerContactIdMapping.get().getUserField());
                        }
                    }
                }
            }

        }
    }
}
