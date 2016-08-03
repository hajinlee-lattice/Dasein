package com.latticeengines.propdata.match.testframework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.service.MetadataColumnService;

@Component("testMatchInputService")
public class TestMatchInputService {

    @Autowired
    private MetadataColumnService<ExternalColumn> externalColumnService;

    public ColumnSelection enrichmentSelection() {
        List<ExternalColumn> columns = new ArrayList<>();
        for (String id : Arrays.asList("TechIndicator_AddThis", "TechIndicator_AdobeCreativeSuite")) {
            columns.add(externalColumnService.getMetadataColumn(id));
        }
        return new ColumnSelection(columns);
    }

}
