package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMapping;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ExternalColumnService;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;

@Component
public class ColumnMetadataServiceImpl implements ColumnMetadataService {

    @Autowired
    private SourceService sourceService;

    @Autowired
    private ExternalColumnService externalColumnService;

    public List<ColumnMetadata> fromPredefinedSelection (ColumnSelection.Predefined selectionName) {
        List<ExternalColumn> externalColumns = externalColumnService.columnSelection(selectionName);
        return toColumnMetadata(externalColumns);
    }

    public List<ColumnMetadata> toColumnMetadata(List<ExternalColumn> externalColumns) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (ExternalColumn externalColumn : externalColumns) {
            try {
                ColumnMetadata columnMetadata = new ColumnMetadata(externalColumn);
                if (externalColumn.getColumnMappings() != null && !externalColumn.getColumnMappings().isEmpty()) {
                    ColumnMapping maxPriorityCM = Collections.max(externalColumn.getColumnMappings(),
                            new Comparator<ColumnMapping>() {
                                public int compare(ColumnMapping cm1, ColumnMapping cm2) {
                                    return Integer.compare(cm1.getPriority(), cm2.getPriority());
                                }
                            });
                    if (maxPriorityCM.getSourceName() != null) {
                        Source source = sourceService.findBySourceName(maxPriorityCM.getSourceName());
                        HasSqlPresence hasSqlPresence = (HasSqlPresence) source;
                        columnMetadata.setMatchDestination(hasSqlPresence.getSqlMatchDestination());
                    }
                }
                columnMetadataList.add(columnMetadata);
            } catch (Exception e) {
                throw new RuntimeException("Failed to extract metadata from ExternalColumn ["
                        + externalColumn.getExternalColumnID() + "]", e);
            }
        }
        return columnMetadataList;
    }

}
