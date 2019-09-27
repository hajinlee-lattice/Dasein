package com.latticeengines.pls.util;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.service.CDLService;

public class SystemIdsUtils {
    private static final Logger log = LoggerFactory.getLogger(SystemIdsUtils.class);

    protected static final String UNIQUE_ID_SECTION = "Unique ID";
    protected static final String MATCH_IDS_SECTION = "Match IDs";
    protected static final String MATCH_TO_ACCOUNT_ID_SECTION = "Match to Accounts - ID";
    protected static final String LATTICE_IDS_SECTION = "Lattice IDs";

    public static void processSystemIds(CustomerSpace customerSpace, String systemName, EntityType entityType,
                                       FieldDefinitionsRecord record, CDLService cdlService) {
        if (!BusinessEntity.Account.equals(entityType.getEntity()) &&
                !BusinessEntity.Contact.equals(entityType.getEntity())) {
            log.info("No System ID support for Business Entity: {}.  Tenant is: {}", entityType.getEntity(),
                    customerSpace.getTenantId());
            return;
        }
        log.info("Processing System IDs for Tenant {}, System Name {}, System Object {}", customerSpace.getTenantId(),
                systemName, entityType.getDisplayName());

        // TODO(jwinter): Evaluate if there is another way to process IDs without hardcoding section names.
        S3ImportSystem currentImportSystem = processUniqueId(customerSpace, systemName, entityType, record,
                UNIQUE_ID_SECTION, cdlService);
        processMatchIds(customerSpace, currentImportSystem, entityType, record, MATCH_IDS_SECTION, cdlService);
        if (BusinessEntity.Contact.equals(entityType.getEntity())) {
            processMatchIds(customerSpace, currentImportSystem, EntityType.Accounts, record,
                    MATCH_TO_ACCOUNT_ID_SECTION, cdlService);
        }
        cdlService.updateS3ImportSystem(customerSpace.toString(), currentImportSystem);
    }

    private static S3ImportSystem processUniqueId(CustomerSpace customerSpace, String systemName, EntityType entityType,
                                        FieldDefinitionsRecord record, String sectionName, CDLService cdlService) {
        S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
        List<FieldDefinition> fieldDefinitionList = record.getFieldDefinitionsRecords(UNIQUE_ID_SECTION);
        if (CollectionUtils.isEmpty(fieldDefinitionList)) {
            return importSystem;
        }
        // For now, we assume there is only one FieldDefinition in this list.
        if (fieldDefinitionList.size() != 1) {
            throw new IllegalArgumentException(sectionName + " section of system name " + systemName + " and object " +
                    entityType.getDisplayName() + " has more than one field");
        }
        FieldDefinition uniqueIdDefinition = fieldDefinitionList.get(0);

        // Ignore the definition is no column is mapped in the current import.
        if (!uniqueIdDefinition.isInCurrentImport()) {
            return importSystem;
        }

        if (BusinessEntity.Account.equals(entityType.getEntity())) {
            // If the field name of the Unique ID Definition is equal to the Spec defined CustomerAccountId, it means
            // that this the first time the user is setting this field.  In this case, we need to update the
            // definition's field name and the import system data.  If this is not the first time the definition is
            // being modified, we only allow changes to the column to field name mapping, which requires the field
            // definition added for the Lattice ID to be updated, if the unique ID maps to the Lattice Account ID.
            if (StringUtils.equals(uniqueIdDefinition.getFieldName(), InterfaceName.CustomerAccountId.name())) {
                if (StringUtils.isBlank(importSystem.getAccountSystemId())) {
                    importSystem.setAccountSystemId(importSystem.generateAccountSystemId());
                }
                uniqueIdDefinition.setFieldName(importSystem.getAccountSystemId());
                importSystem.setMapToLatticeAccount(uniqueIdDefinition.isMappedToLatticeId());
            }

            log.info("State|  entity: {}  section: {}  importSystem: {}  isMappedtoAccount:  {}  " +
                            "isMappedToContact: {}  columnName: {}  fieldName: {}", entityType.getEntity(), sectionName,
                    importSystem.getName(), importSystem.isMapToLatticeAccount(),
                    importSystem.isMapToLatticeContact(), uniqueIdDefinition.getColumnName(),
                    uniqueIdDefinition.getFieldName());

            updateLatticeId(importSystem.isMapToLatticeAccount(), InterfaceName.CustomerAccountId.name(),
                    uniqueIdDefinition.getColumnName(), record);
        } else {
            // As with Account, we check if this is a first time setting of the Unique ID or not, and only update the
            // field name and import system the first time.
            if (StringUtils.equals(uniqueIdDefinition.getFieldName(), InterfaceName.CustomerContactId.name())) {
                if (StringUtils.isBlank(importSystem.getContactSystemId())) {
                    importSystem.setContactSystemId(importSystem.generateContactSystemId());
                }
                uniqueIdDefinition.setFieldName(importSystem.getContactSystemId());
                importSystem.setMapToLatticeContact(uniqueIdDefinition.isMappedToLatticeId());
            }

            log.info("State|  entity: {}  section: {}  importSystem: {}  isMappedtoAccount:  {}  " +
                            "isMappedToContact: {}  columnName: {}  fieldName: {}", entityType.getEntity(), sectionName,
                    importSystem.getName(), importSystem.isMapToLatticeAccount(),
                    importSystem.isMapToLatticeContact(), uniqueIdDefinition.getColumnName(),
                    uniqueIdDefinition.getFieldName());

            updateLatticeId(importSystem.isMapToLatticeContact(), InterfaceName.CustomerContactId.name(),
                    uniqueIdDefinition.getColumnName(), record);
        }
        return importSystem;
    }

    private static void processMatchIds(CustomerSpace customerSpace, S3ImportSystem currentImportSystem,
                                        EntityType entityType, FieldDefinitionsRecord record, String sectionName,
                                        CDLService cdlService) {
        List<FieldDefinition> fieldDefinitionList = record.getFieldDefinitionsRecords(sectionName);
        if (CollectionUtils.isEmpty(fieldDefinitionList)) {
            return;
        }
        for (FieldDefinition matchIdDefinition : fieldDefinitionList) {
            // Ignore the definition is no column is mapped in the current import.
            if (!matchIdDefinition.isInCurrentImport()) {
                continue;
            }

            String systemName = StringUtils.isNotBlank(matchIdDefinition.getExternalSystemName()) ?
                    matchIdDefinition.getExternalSystemName() : currentImportSystem.getName();
            S3ImportSystem importSystem = systemName.equals(currentImportSystem.getName()) ? currentImportSystem :
                    cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
            log.error("systemName: {}  currentSystem:  {}  definitionSystem:  {}", systemName,
                    currentImportSystem.getName(), matchIdDefinition.getExternalSystemName());

            if (BusinessEntity.Account.equals(entityType.getEntity())) {
                if (StringUtils.isBlank(importSystem.getAccountSystemId())) {
                    throw new IllegalStateException("Cannot assign column " + matchIdDefinition.getColumnName() +
                            " as " + entityType.getEntity() + " ID from system " + systemName +
                            " as match ID in section " + sectionName + " before that system has been set up");
                }

                // Only set the field name if it is blank, indicating this is the first time it is being updated.
                if (StringUtils.isBlank(matchIdDefinition.getFieldName())) {
                    matchIdDefinition.setFieldName(importSystem.getAccountSystemId());
                }

                log.info("State|  entity: {}  section: {}  curSystem: {}  defSystem: {}  isMappedtoAccount:  {}  " +
                        "isMappedToContact: {}  columnName: {}  fieldName: {}", entityType.getEntity(), sectionName,
                        currentImportSystem.getName(), importSystem.getName(), importSystem.isMapToLatticeAccount(),
                        importSystem.isMapToLatticeContact(), matchIdDefinition.getColumnName(),
                        matchIdDefinition.getFieldName());

                updateLatticeId(importSystem.isMapToLatticeAccount(), InterfaceName.CustomerAccountId.name(),
                        matchIdDefinition.getColumnName(), record);
            } else {
                if (StringUtils.isBlank(importSystem.getContactSystemId())) {
                    throw new IllegalStateException("Cannot assign column " + matchIdDefinition.getColumnName() +
                            " as " + entityType.getEntity() + " ID from system " +
                            systemName + " as match ID in section " + sectionName +
                            " before that system has been set up");
                }

                // Only set the field name if it is blank, indicating this is the first time it is being updated.
                if (StringUtils.isBlank(matchIdDefinition.getFieldName())) {
                    matchIdDefinition.setFieldName(importSystem.getContactSystemId());
                }

                log.info("State|  entity: {}  section: {}  curSystem: {}  fieldSystem: {}  isMappedtoAccount:  {}  " +
                                "isMappedToContact: {}  columnName: {}  fieldName: {}", entityType.getEntity(), sectionName,
                        currentImportSystem.getName(), importSystem.getName(), importSystem.isMapToLatticeAccount(),
                        importSystem.isMapToLatticeContact(), matchIdDefinition.getColumnName(),
                        matchIdDefinition.getFieldName());

                updateLatticeId(importSystem.isMapToLatticeContact(), InterfaceName.CustomerContactId.name(),
                        matchIdDefinition.getColumnName(), record);
            }
        }
    }

    private static void updateLatticeId(boolean isMappedToLatticeId, String fieldName, String columnName,
                                FieldDefinitionsRecord record) {
        if (isMappedToLatticeId) {
            FieldDefinition latticeIdDefinition = record.getFieldDefinition(LATTICE_IDS_SECTION, fieldName);
            if (latticeIdDefinition == null) {
                latticeIdDefinition = new FieldDefinition();
                latticeIdDefinition.setFieldName(fieldName);
                latticeIdDefinition.setFieldType(UserDefinedType.TEXT);
                latticeIdDefinition.setColumnName(columnName);
                record.addFieldDefinition(LATTICE_IDS_SECTION, latticeIdDefinition, false);

                log.info("Creating new Lattice ID field " + fieldName + " for " + columnName);
            } else {
                latticeIdDefinition.setColumnName(columnName);
                log.info("Updating old Lattice ID field " + fieldName + " which columnName " + columnName);
            }
        }
    }

    /*
    private static void processMatchIdsOld(CustomerSpace customerSpace, String systemName, EntityType entityType,
                                        FieldDefinitionsRecord record, String sectionName, CDLService cdlService) {
        List<FieldDefinition> fieldDefinitionList = record.getFieldDefinitionsRecords(sectionName);
        if (CollectionUtils.isEmpty(fieldDefinitionList)) {
            return;
        }
        for (FieldDefinition matchIdDefinition : fieldDefinitionList) {
            String definitionSystemName = StringUtils.isNotBlank(matchIdDefinition.getExternalSystemName()) ?
                    matchIdDefinition.getExternalSystemName() : systemName;
            S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), definitionSystemName);
            if (BusinessEntity.Account.equals(entityType.getEntity())) {
                if (StringUtils.isBlank(importSystem.getAccountSystemId())) {
                    throw new IllegalStateException("Cannot assign column " + matchIdDefinition.getColumnName() +
                            " as " + entityType.getEntity() + " ID from system " +
                            definitionSystemName + " as match ID in section " + sectionName +
                            " before that system has been set up");
                }

                matchIdDefinition.setFieldName(importSystem.getAccountSystemId());
                updateLatticeId(importSystem.isMapToLatticeAccount(), InterfaceName.CustomerAccountId.name(),
                        matchIdDefinition.getColumnName(), record);
            } else {
                if (StringUtils.isBlank(importSystem.getContactSystemId())) {
                    throw new IllegalStateException("Cannot assign column " + matchIdDefinition.getColumnName() +
                            " as " + entityType.getEntity() + " ID from system " +
                            definitionSystemName + " as match ID in section " + sectionName +
                            " before that system has been set up");
                }
                matchIdDefinition.setFieldName(importSystem.getContactSystemId());
                updateLatticeId(importSystem.isMapToLatticeContact(), InterfaceName.CustomerContactId.name(),
                        matchIdDefinition.getColumnName(), record);
            }
        }
    }

    private static void processMatchToAccountId(CustomerSpace customerSpace, String systemName, EntityType entityType,
                                                FieldDefinitionsRecord record, String sectionName,
                                                CDLService cdlService) {
        List<FieldDefinition> fieldDefinitionList = record.getFieldDefinitionsRecords(sectionName);
        if (CollectionUtils.isEmpty(fieldDefinitionList)) {
            return;
        }

        // For now, we assume there is only one FieldDefinition in this list.
        if (fieldDefinitionList.size() != 1) {
            throw new IllegalStateException(sectionName + " section of system name " + systemName +
                    " and object " + entityType.getDisplayName() + " has more than one field");
        }
        FieldDefinition accountIdDefinition = fieldDefinitionList.get(0);
        String definitionSystemName = StringUtils.isNotBlank(accountIdDefinition.getExternalSystemName()) ?
                accountIdDefinition.getExternalSystemName() : systemName;
        S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(),
                definitionSystemName);

        if (StringUtils.isBlank(importSystem.getAccountSystemId())) {
            throw new IllegalStateException("Cannot assign column " + accountIdDefinition.getColumnName() +
                    " as " + entityType.getEntity() + " ID from system " +
                    definitionSystemName + " as match ID in section " + sectionName +
                    " before that system has been set up");
        }
        accountIdDefinition.setFieldName(importSystem.getAccountSystemId());
        updateLatticeId(importSystem.isMapToLatticeAccount(), InterfaceName.CustomerAccountId.name(),
                accountIdDefinition.getColumnName(), record);\
    }
    */

}
