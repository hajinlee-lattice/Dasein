package com.latticeengines.cdl.workflow.steps.process;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.UpdateSegmentCountResponse;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

final class SegmentCountUtils {

    protected SegmentCountUtils() {
        throw new UnsupportedOperationException();
    }

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

    static List<String> updateEntityCounts(final SegmentProxy segmentProxy, final String customerSpace) {
        UpdateSegmentCountResponse response = segmentProxy.updateSegmentsCounts(customerSpace);
        if (MapUtils.isNotEmpty(response.getUpdatedCounts())) {
            log.info("Updated segment counts: " + JsonUtils.serialize(response.getUpdatedCounts()));
        } else {
            log.info("No updated segment counts.");
        }
        return response.getFailedSegments();
    }

}
