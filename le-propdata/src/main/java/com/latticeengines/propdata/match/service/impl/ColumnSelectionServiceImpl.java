package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ColumnSelectionService;

@Component
public class ColumnSelectionServiceImpl implements ColumnSelectionService {

    @Autowired
    private ColumnMetadataService columnMetadataService;

    private Set<String> excludeColumns = new HashSet<>(
            Arrays.asList("CloudTechnologies_ATS", "CloudTechnologies_SocialMediaMonitoring", "IQC001",
                    "IQC002", "IQC003", "MSA_Code", "RecentPatents", "TotalPatents",
                    "Ultimate_Parent_Company_Indicator"));


    @Override
    public List<ColumnMetadata> getMetaData(ColumnSelection selection) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Map<String, List<String>> getSourceColumnMap(ColumnSelection.Predefined predefined) {
        return getSourceColumnMapFromMetadata(columnMetadataService.fromPredefinedSelection(predefined));
    }

    private Map<String, List<String>> getSourceColumnMapFromMetadata(List<ColumnMetadata> columnMetadata) {
        Map<String, List<String>> map = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        for (ColumnMetadata metadata: columnMetadata) {
            if (metadata.getTagList().contains(ColumnSelection.Predefined.Model.getName())) {
                if (!excludeColumns.contains(metadata.getColumnName())) {
                    columnNames.add(metadata.getColumnName());
                }
            }
        }
        map.put(ColumnSelection.Predefined.Model.getName(), columnNames);
        return map;
    }

}
