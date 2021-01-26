package com.latticeengines.pls.service.vidashboard;

import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.ListSegmentSummary;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;

public interface DashboardService {

    DashboardResponse getDashboardList(String customerSpace);

    MetadataSegment createListSegment(String customerSpace, String segmentName);

    ListSegment updateSegmentFieldMapping(String customerSpace, String segmentName, CSVAdaptor csvAdaptor);

    ListSegmentSummary getListSegmentMappings(String customerSpace, String segmentName);

    ListSegmentSummary getListSegmentSummary(String customerSpace, String segmentName);

    void deleteListSegment(String customerSpace, String segmentName);
}
