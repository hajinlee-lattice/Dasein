package com.latticeengines.pls.util;

import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public final class EntityMatchGAConverterUtils {

    protected static final String DEFAULT_SYSTEM = "DefaultSystem";

    protected EntityMatchGAConverterUtils() {
        throw new UnsupportedOperationException();
    }

    public static void convertGuessingMappings(boolean enableEntityMatch, boolean enableEntityMatchGA,
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
        boolean containsCustomerAccountId = false;
        boolean containsAccountId = false;
        boolean containsCustomerContactId = false;
        boolean containsContactId = false;
        boolean isDefaultSystem = defaultSystem != null && DEFAULT_SYSTEM.equals(defaultSystem.getName());
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (InterfaceName.AccountId.name().equals(fieldMapping.getMappedField())) {
                //add this properties to make sure systemAccountId can be set correctly.
                if (isDefaultSystem) {
                    fieldMapping.setIdType(FieldMapping.IdType.Account);
                    fieldMapping.setMapToLatticeId(true);
                }
                containsAccountId = true;
            }
            if (InterfaceName.ContactId.name().equals(fieldMapping.getMappedField())) {
                //add this properties to make sure systemContactId can be set correctly.
                if (isDefaultSystem) {
                    fieldMapping.setIdType(FieldMapping.IdType.Contact);
                    fieldMapping.setMapToLatticeId(true);
                }
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
                    if (isDefaultSystem) {
                        //add this properties to make sure systemAccountId can be set correctly.
                        fieldMapping.setIdType(FieldMapping.IdType.Account);
                        fieldMapping.setMapToLatticeId(true);
                    }
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
                    if (isDefaultSystem) {
                        //add this properties to make sure systemContactId can be set correctly.
                        fieldMapping.setIdType(FieldMapping.IdType.Contact);
                        fieldMapping.setMapToLatticeId(true);
                    }
                }
            }
        }

        if (isDefaultSystem) {
            if (StringUtils.isNotEmpty(defaultSystem.getAccountSystemId())) {
                fieldMappingDocument.getFieldMappings().removeIf(fieldMapping -> defaultSystem.getAccountSystemId().equals(fieldMapping.getMappedField()));
            }
            if (StringUtils.isNotEmpty(defaultSystem.getContactSystemId())) {
                fieldMappingDocument.getFieldMappings().removeIf(fieldMapping -> defaultSystem.getContactSystemId().equals(fieldMapping.getMappedField()));
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
                    Optional<FieldMapping> systemAccountIdMapping = fieldMappingDocument.getFieldMappings().stream()
                            .filter(fieldMapping -> defaultSystem.getAccountSystemId().equals(fieldMapping.getMappedField()))
                            .findAny();
                    if (systemAccountIdMapping.isPresent()) {
                        systemAccountIdMapping.get().setUserField(customerAccountIdMapping.get().getUserField());
                    } else {
                        FieldMapping fieldMapping = new FieldMapping();
                        fieldMapping.setUserField(customerAccountIdMapping.get().getUserField());
                        fieldMapping.setMappedField(defaultSystem.getAccountSystemId());
                        fieldMapping.setFieldType(customerAccountIdMapping.get().getFieldType());
                        fieldMappingDocument.getFieldMappings().add(fieldMapping);
                    }
                }
            }
            if (StringUtils.isNotEmpty(defaultSystem.getContactSystemId())) {
                Optional<FieldMapping> customerContactIdMapping = fieldMappingDocument.getFieldMappings().stream()
                        .filter(fieldMapping -> InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()))
                        .findAny();
                if (customerContactIdMapping.isPresent()) {
                    Optional<FieldMapping> systemContactId = fieldMappingDocument.getFieldMappings().stream()
                            .filter(fieldMapping -> defaultSystem.getContactSystemId().equals(fieldMapping.getMappedField()))
                            .findAny();
                    if (systemContactId.isPresent()) {
                        systemContactId.get().setUserField(customerContactIdMapping.get().getUserField());
                    } else {
                        FieldMapping fieldMapping = new FieldMapping();
                        fieldMapping.setUserField(customerContactIdMapping.get().getUserField());
                        fieldMapping.setMappedField(defaultSystem.getContactSystemId());
                        fieldMapping.setFieldType(customerContactIdMapping.get().getFieldType());
                        fieldMappingDocument.getFieldMappings().add(fieldMapping);
                    }
                }
            }

        }
    }
}
