package com.latticeengines.apps.cdl.mds.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

import reactor.core.publisher.Flux;

@Component
public class RatingDisplayMetadataStoreImpl implements RatingDisplayMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(RatingDisplayMetadataStoreImpl.class);

    private final TenantEntityMgr tenantEntityMgr;

    private final RatingEngineService ratingEngineService;

    @Inject
    public RatingDisplayMetadataStoreImpl(TenantEntityMgr tenantEntityMgr, RatingEngineService ratingEngineService) {
        this.tenantEntityMgr = tenantEntityMgr;
        this.ratingEngineService = ratingEngineService;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace1<String> namespace) {
        List<RatingEngineSummary> ratingEngineSummaries = getRatingSummaries(namespace.getCoord1());
        Flux<ColumnMetadata> cms = Flux.empty();
        if (CollectionUtils.isNotEmpty(ratingEngineSummaries)) {
            List<String> suffixes = new ArrayList<>();
            suffixes.add("");
            suffixes.addAll(RatingEngine.SCORE_ATTR_SUFFIX.values().stream().map(s -> "_" + s)
                    .collect(Collectors.toList()));
            cms = Flux.fromIterable(ratingEngineSummaries).parallel().runOn(ThreadPoolUtils.getMdsScheduler())
                    .flatMap(summary -> {
                        String segmentDisplayName = summary.getSegmentDisplayName();
                        String reDisplayName = summary.getDisplayName();
                        String engineNameStem = RatingEngine.toRatingAttrName(summary.getId());
                        List<ColumnMetadata> reAttrs = suffixes.stream().map(suffix -> {
                            String attrName = engineNameStem + suffix;
                            ColumnMetadata reAttr = new ColumnMetadata();
                            reAttr.setAttrName(attrName);
                            reAttr.setDisplayName(reDisplayName);
                            reAttr.setSecondaryDisplayName(getSecondaryDisplayName(suffix));
                            reAttr.setSubcategory(segmentDisplayName);
                            reAttr.setCategory(Category.RATING);
                            if (isSegmentable(suffix)) {
                                reAttr.enableGroup(ColumnSelection.Predefined.Segment);
                            } else {
                                reAttr.disableGroup(ColumnSelection.Predefined.Segment);
                            }
                            return reAttr;
                        }).collect(Collectors.toList());
                        return Flux.fromIterable(reAttrs);
                    }).sequential();
        }
        return cms;
    }

    private String getSecondaryDisplayName(String suffix) {
        String secondaryDisplayName = null;
        if (StringUtils.isBlank(suffix)) {
            secondaryDisplayName = "Rating";
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.ExpectedRevenue).equalsIgnoreCase(suffix.substring(1))) {
            secondaryDisplayName = "Weighted Revenue";
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.NormalizedScore).equalsIgnoreCase(suffix.substring(1))) {
            secondaryDisplayName = "Score";
        }
        return secondaryDisplayName;
    }

    private boolean isSegmentable(String suffix) {
        boolean segmentable = false;
        if (StringUtils.isBlank(suffix)) {
            segmentable = true;
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.ExpectedRevenue).equalsIgnoreCase(suffix.substring(1))) {
            segmentable = true;
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.NormalizedScore).equalsIgnoreCase(suffix.substring(1))) {
            segmentable = true;
        }
        return segmentable;
    }

    private List<RatingEngineSummary> getRatingSummaries(String tenantId) {
        String fullId = CustomerSpace.parse(tenantId).toString();
        String tenantIdInContext = MultiTenantContext.getTenantId();
        if (!CustomerSpace.parse(tenantIdInContext).toString().equals(fullId)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(fullId);
            if (tenant == null) {
                throw new IllegalArgumentException("Cannot find tenant with id " + tenantId);
            }
            MultiTenantContext.setTenant(tenant);
        }
        List<RatingEngineSummary> engineSummaries = new ArrayList<>();
        try {
            engineSummaries = ratingEngineService.getAllRatingEngineSummaries();
        } catch (Exception e) {
            log.warn("Failed to retrieve engine summaries.", e);
        }
        return engineSummaries;
    }

}
