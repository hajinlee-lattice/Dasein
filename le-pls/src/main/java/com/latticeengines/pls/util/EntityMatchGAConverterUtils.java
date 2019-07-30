package com.latticeengines.pls.util;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class EntityMatchGAConverterUtils {

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
        boolean containsCustomerContactId = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField()) ||
                    InterfaceName.ContactId.name().equals(fieldMapping.getMappedField())) {
                return;
            }
            if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                containsCustomerAccountId = true;
            }
            if (InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField())) {
                containsCustomerContactId = true;
            }
        }
        if (containsCustomerAccountId) {
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setMappedField(InterfaceName.AccountId.name());
                }
            }
        }
        if (containsCustomerContactId) {
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setMappedField(InterfaceName.ContactId.name());
                }
            }
        }
    }

    public static void convertSavingMappings(boolean enableEntityMatch, boolean enableEntityMatchGA,
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
        boolean containsAccountId = false;
        boolean containsContactId = false;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                containsAccountId = true;
            }
            if (InterfaceName.ContactId.name().equals(fieldMapping.getMappedField())) {
                containsContactId = true;
            }
        }
        if (containsAccountId) {
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setMappedField(InterfaceName.CustomerAccountId.name());
                }
            }
        }
        if (containsContactId) {
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (InterfaceName.ContactId.name().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setMappedField(InterfaceName.CustomerContactId.name());
                }
            }
        }
    }
}
