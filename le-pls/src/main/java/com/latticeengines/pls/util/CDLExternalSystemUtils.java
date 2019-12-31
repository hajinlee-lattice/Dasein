package com.latticeengines.pls.util;

import static com.latticeengines.common.exposed.util.AvroUtils.getAvroFriendlyString;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;

public class CDLExternalSystemUtils {

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
                // setting for field name
                if (!definition.getFieldName().toUpperCase().endsWith("ID")) {
                    definition.setFieldName(definition.getExternalSystemName() + "_ID");
                }
                String externalAttrName = getAvroFriendlyString(definition.getFieldName());
                String externalDisplayName = definition.getColumnName();
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
                }
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
}
