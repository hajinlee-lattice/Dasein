package com.latticeengines.matchapi.testframework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("testMatchInputService")
public class TestMatchInputService {

    @Autowired
    private MetadataColumnService<ExternalColumn> externalColumnService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    public MatchInput prepareSimpleRTSMatchInput(Object[][] data) {
        return TestMatchInputUtils.prepareSimpleMatchInput(data);
    }

    public MatchInput prepareSimpleAMMatchInput(Object[][] data) {
        MatchInput input = prepareSimpleRTSMatchInput(data);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        return input;
    }

    public MatchInput prepareSimpleRTSMatchInput(List<List<Object>> mockData) {
        return TestMatchInputUtils.prepareSimpleMatchInput(mockData);
    }

    public MatchInput prepareSimpleAMMatchInput(List<List<Object>> mockData) {
        MatchInput input = prepareSimpleRTSMatchInput(mockData);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        return input;
    }

    public ColumnSelection enrichmentSelection() {
        List<ExternalColumn> columns = new ArrayList<>();
        for (String id : Arrays.asList("TechIndicator_AddThis", "TechIndicator_AdobeCreativeSuite")) {
            columns.add(externalColumnService.getMetadataColumn(id,
                    versionEntityMgr.currentApprovedVersion().getVersion()));
        }

        ColumnSelection cs = new ColumnSelection();
        cs.createColumnSelection(columns);
        return cs;
    }

}
