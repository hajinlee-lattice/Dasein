package com.latticeengines.propdata.api.controller.microservice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMapping;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.propdata.core.service.ExternalColumnService;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "columnmetadata", description = "REST resource for column metadata")
@RestController
@RequestMapping("/metadata")
public class ColumnMetadataResource implements ColumnMetadataInterface {

    @Autowired
    private ExternalColumnService externalColumnService;

    @Autowired
    private SourceService sourceService;

    @RequestMapping(value = "/predefined/{selectName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "selectName for lead enrichment metadata is LEAD_ENRICHMENT (case-sensitive)")
    @Override
    public List<ColumnMetadata> columnSelection(@PathVariable ColumnSelection.Predefined selectName) {
        List<ExternalColumn> externalColumns = externalColumnService.columnSelection(selectName);
        return columnMetadataWrapper(externalColumns);
    }

    public List<ColumnMetadata> columnMetadataWrapper(List<ExternalColumn> externalColumns) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<ColumnMetadata>();
        for (ExternalColumn externalColumn : externalColumns) {
            ColumnMetadata columnMetadata = new ColumnMetadata(externalColumn);
            if (externalColumn.getColumnMappings() != null && externalColumn.getColumnMappings().size() != 0) {
                ColumnMapping maxPiorityCM = Collections.max(externalColumn.getColumnMappings(),
                        new Comparator<ColumnMapping>() {
                            public int compare(ColumnMapping cm1, ColumnMapping cm2) {
                                return Integer.compare(cm1.getPriority(), cm2.getPriority());
                            }
                        });
                if (maxPiorityCM.getSourceName() != null) {
                    Source source = sourceService.findBySourceName(maxPiorityCM.getSourceName());
                    HasSqlPresence hasSqlPresence = (HasSqlPresence) source;
                    columnMetadata.setMatchDestination(hasSqlPresence.getSqlMatchDestination());
                }
            }
            columnMetadataList.add(columnMetadata);
        }
        return columnMetadataList;
    }
}
