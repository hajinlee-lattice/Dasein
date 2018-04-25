package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.pls.service.AttrConfigService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

@Component("attrConfigService")
public class AttrConfigServiceImpl implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImpl.class);

    private static final String[] properties = { ColumnSelection.Predefined.Segment.getName(),
            ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.TalkingPoint.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName() };

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @SuppressWarnings("unchecked")
    @Override
    public AttrConfigActivationOverview getAttrConfigActivationOverview(Category category) {
        List<AttrConfigOverview<?>> list = cdlAttrConfigProxy.getAttrConfigOverview(MultiTenantContext.getTenantId(),
                category.getName(), ColumnMetadataKey.State);
        AttrConfigOverview<AttrState> attrCategoryOverview = (AttrConfigOverview<AttrState>) list.get(0);
        AttrConfigActivationOverview categoryOverview = new AttrConfigActivationOverview();
        categoryOverview.setCategory(attrCategoryOverview.getCategory());
        categoryOverview.setLimit(attrCategoryOverview.getLimit());
        categoryOverview.setTotalAttrs(attrCategoryOverview.getTotalAttrs());
        categoryOverview
                .setSelected(attrCategoryOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active));
        return categoryOverview;
    }

    @Override
    public AttrConfigUsageOverview getAttrConfigUsageOverview() {
        AttrConfigUsageOverview usageOverview = new AttrConfigUsageOverview();
        Map<String, Long> attrNums = new HashMap<>();
        Map<String, Map<String, Long>> selections = new HashMap<>();
        usageOverview.setAttrNums(attrNums);
        usageOverview.setSelections(selections);
        // can be improved by multithreading
        int count = 0;
        for (String property : properties) {
            List<AttrConfigOverview<?>> list = cdlAttrConfigProxy
                    .getAttrConfigOverview(MultiTenantContext.getTenantId(), null, property);
            log.info("list is " + list);
            Map<String, Long> detailedSelections = new HashMap<>();
            if (property.equals(ColumnSelection.Predefined.Enrichment.getName())) {
                // TODO update the limit for enrich/export
                detailedSelections.put(AttrConfigUsageOverview.LIMIT, 100L);
            }
            selections.put(property, detailedSelections);
            long num = 0L;
            for (AttrConfigOverview<?> attrConfigOverview : list) {
                // For each category, update its total attrNums
                if (count == 0) {
                    if (attrConfigOverview.getCategory().isPremium()) {
                        AttrConfigActivationOverview categoryOverview = getAttrConfigActivationOverview(
                                attrConfigOverview.getCategory());
                        attrNums.put(attrConfigOverview.getCategory().getName(), categoryOverview.getSelected());
                    } else {
                        attrNums.put(attrConfigOverview.getCategory().getName(), attrConfigOverview.getTotalAttrs());
                    }
                }

                // Synthesize the properties for all the categories
                Map<?, Long> propertyDetail = attrConfigOverview.getPropSummary().get(property);
                if (propertyDetail != null && propertyDetail.get(Boolean.TRUE) != null) {
                    num += propertyDetail.get(Boolean.TRUE);
                }
                detailedSelections.put(AttrConfigUsageOverview.SELECTED, num);
            }
            count++;

        }
        return usageOverview;
    }

    @Override
    public void updateActivationConfig(String categoryName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getTenantId();
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForActivation(request);
        cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForActivation(AttrConfigSelectionRequest request) {
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        for (String attr : request.getSelect()) {
            updateAttrConfigs(attrConfigs, attr, ColumnMetadataKey.State, Boolean.TRUE);
        }
        for (String attr : request.getDeselect()) {
            updateAttrConfigs(attrConfigs, attr, ColumnMetadataKey.State, Boolean.FALSE);
        }
        return attrConfigRequest;
    }

    private void updateAttrConfigs(List<AttrConfig> attrConfigs, String attrName, String property,
            Boolean selectThisAttr) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);
        config.setEntity(BusinessEntity.Account);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        config.setAttrProps(ImmutableMap.of(property, enrichProp));
        attrConfigs.add(config);
    }

    @Override
    public void updateUsageConfig(String categoryName, String usage, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getTenantId();
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForUsage(usage, request);
        cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForUsage(String usage, AttrConfigSelectionRequest request) {
        String property = translateUsageToProperty(usage);
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        for (String attr : request.getSelect()) {
            updateAttrConfigs(attrConfigs, attr, property, Boolean.TRUE);
        }
        for (String attr : request.getDeselect()) {
            updateAttrConfigs(attrConfigs, attr, property, Boolean.FALSE);
        }
        return attrConfigRequest;
    }

    @VisibleForTesting
    String translateUsageToProperty(String usage) {
        if (usage.equalsIgnoreCase("SEGMENTATION")) {
            return ColumnSelection.Predefined.Segment.getName();
        } else if (usage.equalsIgnoreCase("EXPORT")) {
            return ColumnSelection.Predefined.Enrichment.getName();
        } else if (usage.equalsIgnoreCase("TALKING POINTS")) {
            return ColumnSelection.Predefined.TalkingPoint.getName();
        } else if (usage.equalsIgnoreCase("COMPANY PROFILE")) {
            return ColumnSelection.Predefined.CompanyProfile.getName();
        } else {
            throw new IllegalArgumentException(String.format("%s is not a valid usage", usage));
        }
    }

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName) {
        return getAttrConfigSelectionDetails(categoryName, ColumnMetadataKey.State);
    }

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryName, String usage) {
        AttrConfigSelectionDetail attrConfigSelectionDetail = new AttrConfigSelectionDetail();
        Category category = resolveCategory(categoryName);
        String property = translateUsageToProperty(usage);

        return attrConfigSelectionDetail;
    }

    private Category resolveCategory(String categoryName) {
        Category category = Category.fromName(categoryName);
        if (category == null) {
            throw new IllegalArgumentException("Cannot parse category " + categoryName);
        }
        return category;
    }

}
