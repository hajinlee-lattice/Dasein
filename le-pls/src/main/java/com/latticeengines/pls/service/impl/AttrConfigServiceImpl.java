package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
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

}
