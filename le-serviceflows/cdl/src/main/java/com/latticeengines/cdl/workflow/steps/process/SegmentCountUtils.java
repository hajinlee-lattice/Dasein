package com.latticeengines.cdl.workflow.steps.process;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

final class SegmentCountUtils {

    private static final Logger log = LoggerFactory.getLogger(SegmentCountUtils.class);

    // to refresh cache
    static void invokeMetadataApi(final ServingStoreProxy servingStoreProxy, final String customerSpace) {
        try {
            List<ColumnMetadata> cms = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.Account);
            log.info("Fetched " + CollectionUtils.size(cms) + " decorated metadata for " + customerSpace);
        } catch (Exception e) {
            log.warn("Failed to fetching decorated metadata for " + customerSpace);
        }
    }

    static void updateEntityCounts(final SegmentProxy segmentProxy, final EntityProxy entityProxy,
            final String customerSpace) {
        List<MetadataSegment> segments = segmentProxy.getMetadataSegments(customerSpace);
        // no need to go parallel here
        // because the concurrency level is limited on redshift side
        if (CollectionUtils.isNotEmpty(segments)) {
            log.info("Going to update " + segments.size() + " segments.");
            segments.forEach(segment -> {
                // use a deep copy to avoid changing restriction format to break UI
                MetadataSegment segmentCopy = JsonUtils.deserialize(JsonUtils.serialize(segment), MetadataSegment.class);
                for (BusinessEntity entity : BusinessEntity.COUNT_ENTITIES) {
                    try {
                        Long count = getEntityCount(entityProxy, customerSpace, entity, segmentCopy);
                        segment.setEntityCount(entity, count);
                        log.info("Set " + entity + " count of segment " + segment.getName() + " to " + count);
                    } catch (Exception e) {
                        log.error("Failed to get " + entity + " count for segment " + segment.getName());
                    }
                    segmentProxy.createOrUpdateSegment(customerSpace, segment);
                }
            });
        } else {
            log.info("There is no segment to update.");
        }
    }

    private static Long getEntityCount(final EntityProxy entityProxy, final String customerSpace,
            final BusinessEntity entity, final MetadataSegment segment) {
        if (segment == null) {
            return null;
        }
        FrontEndQuery frontEndQuery = segment.toFrontEndQuery(entity);
        return entityProxy.getCount(customerSpace, frontEndQuery);
    }

}
