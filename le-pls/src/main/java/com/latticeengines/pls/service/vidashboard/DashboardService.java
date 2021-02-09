package com.latticeengines.pls.service.vidashboard;

import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.cdl.dashboard.TargetAccountList;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;

public interface DashboardService {

    DashboardResponse getDashboardList(String customerSpace);

    MetadataSegment createTargetAccountList(String customerSpace, String listName);

    TargetAccountList getTargetAccountList(String customerSpace, String listName);

    ListSegment updateTargetAccountListMapping(String customerSpace, String listName, CSVAdaptor mapping);

    void deleteTargetAccountList(String customerSpace, String listName);
}
