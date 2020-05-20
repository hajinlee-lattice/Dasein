package com.latticeengines.pls.util;

import static com.latticeengines.common.exposed.util.AvroUtils.getAvroFriendlyString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;

public final class CDLExternalSystemUtils {

    protected CDLExternalSystemUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(CDLExternalSystemUtils.class);

    public static CDLExternalSystem processOtherID(EntityType entityType, FieldDefinitionsRecord commitRequest) {
        List<FieldDefinition> otherIdDefinitions =
                commitRequest.getFieldDefinitionsRecords(FieldDefinitionSectionName.Other_IDs.getName());
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        List<String> mapIds = new ArrayList<>();
        List<String> erpIds = new ArrayList<>();
        List<String> otherIds = new ArrayList<>();
        List<Pair<String, String>> idMappings = new ArrayList<>();
        for (FieldDefinition definition : otherIdDefinitions) {
            if (definition.getExternalSystemType() != null && StringUtils.isNotBlank(definition.getFieldName())) {
                String externalDisplayName = definition.getFieldName();
                // setting for field name
                if (!definition.getFieldName().toUpperCase().endsWith("ID")) {
                    definition.setFieldName(definition.getFieldName() + "_ID");
                }
                String externalAttrName = getAvroFriendlyString(definition.getFieldName());
                if (!externalAttrName.startsWith(ImportWorkflowSpecUtils.USER_PREFIX)) {
                    externalAttrName = ImportWorkflowSpecUtils.USER_PREFIX + externalAttrName;
                }
                idMappings.add(Pair.of(externalAttrName, externalDisplayName));
                switch (definition.getExternalSystemType()) {
                    case CRM:
                        crmIds.add(externalAttrName);
                        break;
                    case MAP:
                        mapIds.add(externalAttrName);
                        break;
                    case ERP:
                        erpIds.add(externalAttrName);
                        break;
                    case OTHER:
                        otherIds.add(externalAttrName);
                        break;
                    default:
                }
                // at generating table step, the Attribute name for other field definitions should begin with "user_"
                definition.setFieldName(externalAttrName);
            } else {
                log.info("skip the cdl external setting for " + definition.getColumnName());
            }
        }
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setMAPIdList(mapIds);
        cdlExternalSystem.setERPIdList(erpIds);
        cdlExternalSystem.setOtherIdList(otherIds);
        cdlExternalSystem.setIdMapping(idMappings);
        return cdlExternalSystem;
    }

    public static void setCDLExternalSystem(CDLExternalSystem newExternalSystem, BusinessEntity entity,
                                            CDLExternalSystemProxy cdlExternalSystemProxy) {
        if (newExternalSystem == null ||
                (CollectionUtils.isEmpty(newExternalSystem.getCRMIdList())
                        && CollectionUtils.isEmpty(newExternalSystem.getERPIdList())
                        && CollectionUtils.isEmpty(newExternalSystem.getMAPIdList())
                        && CollectionUtils.isEmpty(newExternalSystem.getOtherIdList()))) {
            return;
        }
        CDLExternalSystem originalSystem = cdlExternalSystemProxy
                .getCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(), entity.name());
        if (originalSystem == null) {
            CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
            cdlExternalSystem.setCRMIdList(newExternalSystem.getCRMIdList());
            cdlExternalSystem.setMAPIdList(newExternalSystem.getMAPIdList());
            cdlExternalSystem.setERPIdList(newExternalSystem.getERPIdList());
            cdlExternalSystem.setOtherIdList(newExternalSystem.getOtherIdList());
            cdlExternalSystem.setIdMapping(newExternalSystem.getIdMapping());
            cdlExternalSystem.setEntity(entity);
            cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(),
                    cdlExternalSystem);
        } else {
            originalSystem.setCRMIdList(mergeList(originalSystem.getCRMIdList(), newExternalSystem.getCRMIdList()));
            originalSystem.setMAPIdList(mergeList(originalSystem.getMAPIdList(), newExternalSystem.getMAPIdList()));
            originalSystem.setERPIdList(mergeList(originalSystem.getERPIdList(), newExternalSystem.getERPIdList()));
            originalSystem.setOtherIdList(mergeList(originalSystem.getOtherIdList(), newExternalSystem.getOtherIdList()));
            originalSystem.addIdMapping(newExternalSystem.getIdMappingList());
            originalSystem.setEntity(originalSystem.getEntity());
            cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(),
                    originalSystem);
        }
    }

    private static List<String> mergeList(List<String> list1, List<String> list2) {
        if (CollectionUtils.isEmpty(list1)) {
            return list2;
        } else if (CollectionUtils.isEmpty(list2)) {
            return list1;
        }
        List<String> merged = new ArrayList<>(list1);
        Set<String> list1Set = new HashSet<>(list1);
        list2.forEach(item -> {
            if (!list1Set.contains(item)) {
                list1Set.add(item);
                merged.add(item);
            }
        });
        return merged;
    }
}
