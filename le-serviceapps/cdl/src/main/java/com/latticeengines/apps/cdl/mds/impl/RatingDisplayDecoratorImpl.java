package com.latticeengines.apps.cdl.mds.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.RatingDisplayDecorator;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.MapDecorator;
import com.latticeengines.domain.exposed.metadata.mds.SingletonDecoratorFactory;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

import reactor.core.publisher.Flux;

@Component("ratingDisplayDecorator")
public class RatingDisplayDecoratorImpl extends SingletonDecoratorFactory implements RatingDisplayDecorator {

    private static final Logger log = LoggerFactory.getLogger(RatingDisplayDecoratorImpl.class);

    @Inject
    public RatingDisplayDecoratorImpl(RatingEngineProxy ratingEngineProxy) {
        super(new InnerDecorator(ratingEngineProxy));
    }

    private static class InnerDecorator extends MapDecorator implements Decorator {

        private final RatingEngineProxy ratingEngineProxy;

        private InnerDecorator(RatingEngineProxy ratingEngineProxy) {
            super("RatingDisplayDecorator");
            this.ratingEngineProxy = ratingEngineProxy;
        }

        @Override
        protected List<ColumnMetadata> loadInternal() {
            String tenantId = MultiTenantContext.getTenantId();
            List<RatingEngineSummary> ratingEngineSummaries = getRatingSummaries(tenantId);
            List<ColumnMetadata> cms = new ArrayList<>();
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
                                reAttr.setSubcategory(segmentDisplayName);
                                reAttr.setCategory(Category.RATING);
                                return reAttr;
                            }).collect(Collectors.toList());
                            return Flux.fromIterable(reAttrs);
                        }).sequential().collectList().block();
            }
            return cms;
        }

        private List<RatingEngineSummary> getRatingSummaries(String customerSpace) {
            List<RatingEngineSummary> engineSummaries = new ArrayList<>();
            try {
                engineSummaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
            } catch (Exception e) {
                log.warn("Failed to retrieve engine summaries.", e);
            }
            return engineSummaries;
        }

    }

}
