package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.frontend.ExternalSystemMapping;
import com.latticeengines.pls.service.ExternalSystemService;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

@Service("externalSystemService")
public class ExternalSystemServiceImpl implements ExternalSystemService {

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Override
    public List<ExternalSystemMapping> getExternalSystemMappings(String entity, String source, String feedType,
                                                                 Boolean includeAll) {
        Preconditions.checkNotNull(MultiTenantContext.getCustomerSpace());
        Preconditions.checkArgument("Account".equalsIgnoreCase(entity) || "Contact".equalsIgnoreCase(entity));
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        CDLExternalSystem cdlExternalSystem = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace, entity);
        if (cdlExternalSystem == null) {
            return Collections.emptyList();
        }
        Map<String, Attribute> attrFromTemplate = new HashMap<>();
        Map<String, Attribute> attrFromOther = new HashMap<>();
        if (Boolean.TRUE.equals(includeAll)) {
            List<DataFeedTask> allTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(allTasks)) {
                for (DataFeedTask dataFeedTask : allTasks) {
                    if (dataFeedTask.getEntity().equalsIgnoreCase(entity)
                            && dataFeedTask.getSource().equalsIgnoreCase(source)
                            && dataFeedTask.getFeedType().equalsIgnoreCase(feedType)) {
                        attrFromTemplate = dataFeedTask.getImportTemplate().getAttributes().stream()
                                .collect(Collectors.toMap(Attribute::getName, attr -> attr));
                    } else {
                        attrFromOther.putAll(dataFeedTask.getImportTemplate().getAttributes().stream()
                                .collect(Collectors.toMap(Attribute::getName, attr -> attr)));
                    }
                }
            }
        } else {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, source, feedType, entity);
            if (dataFeedTask != null) {
                attrFromTemplate = dataFeedTask.getImportTemplate().getAttributes().stream()
                        .collect(Collectors.toMap(Attribute::getName, attr -> attr));
            }
        }
        List<ExternalSystemMapping> externalSystemMappings = new ArrayList<>();
        getExternalSystemMappingByType(cdlExternalSystem, attrFromTemplate, attrFromOther, externalSystemMappings,
                CDLExternalSystemType.CRM);
        getExternalSystemMappingByType(cdlExternalSystem, attrFromTemplate, attrFromOther, externalSystemMappings,
                CDLExternalSystemType.MAP);
        getExternalSystemMappingByType(cdlExternalSystem, attrFromTemplate, attrFromOther, externalSystemMappings,
                CDLExternalSystemType.ERP);
        getExternalSystemMappingByType(cdlExternalSystem, attrFromTemplate, attrFromOther, externalSystemMappings,
                CDLExternalSystemType.OTHER);


        return externalSystemMappings;
    }

    private void getExternalSystemMappingByType(CDLExternalSystem cdlExternalSystem,
                                                Map<String, Attribute> attrFromTemplate,
                                                Map<String, Attribute> attrFromOther,
                                                List<ExternalSystemMapping> externalSystemMappings,
                                                CDLExternalSystemType type) {
        List<String> idList;
        switch (type) {
            case CRM:
                idList = cdlExternalSystem.getCRMIdList();
                break;
            case MAP:
                idList = cdlExternalSystem.getMAPIdList();
                break;
            case ERP:
                idList = cdlExternalSystem.getERPIdList();
                break;
            case OTHER:
                idList = cdlExternalSystem.getOtherIdList();
                break;
            default:
                idList = Collections.emptyList();
                break;
        }
        if (CollectionUtils.isNotEmpty(idList)) {
            for (String id : idList) {
                if (attrFromTemplate.containsKey(id)) {
                    String nameFromFile = attrFromTemplate.get(id).getSourceAttrName() != null ?
                            attrFromTemplate.get(id).getSourceAttrName() : attrFromTemplate.get(id).getDisplayName();
                    externalSystemMappings.add(getExternalSystemMapping(cdlExternalSystem.getDisplayNameById(id),
                            nameFromFile, type, true));
                } else if (attrFromOther.containsKey(id)) {
                    String nameFromFile = attrFromOther.get(id).getSourceAttrName() != null ?
                            attrFromOther.get(id).getSourceAttrName() : attrFromOther.get(id).getDisplayName();
                    externalSystemMappings.add(getExternalSystemMapping(cdlExternalSystem.getDisplayNameById(id),
                            nameFromFile, type, false));
                }
            }
        }
    }

    private ExternalSystemMapping getExternalSystemMapping(String displayName, String nameFromFile,
                                                           CDLExternalSystemType type, Boolean inCurrentTemplate) {
        ExternalSystemMapping mapping = new ExternalSystemMapping();
        mapping.setAttrDisplayName(displayName);
        mapping.setNameFromFile(nameFromFile);
        mapping.setSystemType(type);
        mapping.setInCurrentTemplate(inCurrentTemplate);
        return mapping;
    }
}
