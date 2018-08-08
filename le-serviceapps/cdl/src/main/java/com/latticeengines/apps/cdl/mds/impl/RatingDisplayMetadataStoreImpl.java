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
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;

@Component
public class RatingDisplayMetadataStoreImpl implements RatingDisplayMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(RatingDisplayMetadataStoreImpl.class);

    private final CDLNamespaceService cdlNamespaceService;
    private final RatingEngineService ratingEngineService;

    private static final List<String> SUFFIXES = new ArrayList<>();

    static {
        SUFFIXES.clear();
        SUFFIXES.add("");
        SUFFIXES.addAll(
                RatingEngine.SCORE_ATTR_SUFFIX.values().stream().map(s -> "_" + s).collect(Collectors.toList()));
    }

    @Inject
    public RatingDisplayMetadataStoreImpl(CDLNamespaceService cdlNamespaceService, RatingEngineService ratingEngineService) {
        this.cdlNamespaceService = cdlNamespaceService;
        this.ratingEngineService = ratingEngineService;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace1<String> namespace) {
        Flux<ColumnMetadata> cms = Flux.empty();
        String tenantId = CustomerSpace.shortenCustomerSpace(namespace.getCoord1());
        if (StringUtils.isNotBlank(tenantId)) {
            List<RatingEngineSummary> ratingEngineSummaries = getRatingSummaries(tenantId);
            if (CollectionUtils.isNotEmpty(ratingEngineSummaries)) {
                log.info("Loading rating display metadata from " + ratingEngineSummaries.size()
                        + " rating engines for tenant " + tenantId);
                cms = Flux.fromIterable(ratingEngineSummaries).concatMap(this::expandEngine);
            }
        }
        return cms;
    }

    private Flux<ColumnMetadata> expandEngine(RatingEngineSummary summary) {
        String segmentDisplayName = summary.getSegmentDisplayName();
        String reDisplayName = summary.getDisplayName();
        String engineNameStem = RatingEngine.toRatingAttrName(summary.getId());
        return Flux.fromIterable(SUFFIXES).map(suffix -> {
            String attrName = engineNameStem + suffix;
            ColumnMetadata reAttr = new ColumnMetadata();
            reAttr.setAttrName(attrName);
            reAttr.setDisplayName(reDisplayName + " " + getSecondaryDisplayName(suffix));
            reAttr.setSubcategory(segmentDisplayName);
            reAttr.setCategory(Category.RATING);
            if (summary.getDeleted()) {
                reAttr.setAttrState(AttrState.Inactive);
            } else if (RatingEngineStatus.INACTIVE.equals(summary.getStatus())) {
                reAttr.setAttrState(AttrState.Deprecated);
                reAttr.setShouldDeprecate(true);
            }
            reAttr.setCanSegment(true);
            if (isSegmentable(suffix)) {
                reAttr.enableGroup(ColumnSelection.Predefined.Segment);
            } else {
                reAttr.disableGroup(ColumnSelection.Predefined.Segment);
            }
            reAttr.setCanEnrich(true);
            if (isExportByDefault(suffix)) {
                reAttr.enableGroup(ColumnSelection.Predefined.Enrichment);
            } else {
                reAttr.disableGroup(ColumnSelection.Predefined.Enrichment);
            }
            reAttr.disableGroup(ColumnSelection.Predefined.CompanyProfile);
            return reAttr;
        });
    }

    private String getSecondaryDisplayName(String suffix) {
        String secondaryDisplayName = null;
        if (StringUtils.isBlank(suffix)) {
            secondaryDisplayName = "Rating";
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.ExpectedRevenue)
                .equalsIgnoreCase(suffix.substring(1))) {
            secondaryDisplayName = "Weighted Revenue";
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.Score)
                .equalsIgnoreCase(suffix.substring(1))) {
            secondaryDisplayName = "Score";
        }
        return secondaryDisplayName;
    }

    private boolean isSegmentable(String suffix) {
        boolean segmentable = false;
        if (StringUtils.isBlank(suffix)) {
            segmentable = true;
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.ExpectedRevenue)
                .equalsIgnoreCase(suffix.substring(1))) {
            segmentable = true;
        } else if (RatingEngine.SCORE_ATTR_SUFFIX.get(RatingEngine.ScoreType.Score)
                .equalsIgnoreCase(suffix.substring(1))) {
            segmentable = false;
        }
        return segmentable;
    }

    private boolean isExportByDefault(String suffix) {
        return StringUtils.isBlank(suffix);
    }

    private List<RatingEngineSummary> getRatingSummaries(String tenantId) {
        cdlNamespaceService.setMultiTenantContext(tenantId);
        List<RatingEngineSummary> engineSummaries = new ArrayList<>();
        try {
            engineSummaries = ratingEngineService.getAllRatingEngineSummaries();
        } catch (Exception e) {
            log.warn("Failed to retrieve engine summaries.", e);
        }
        return engineSummaries;
    }

}
