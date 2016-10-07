package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.entitymgr.ColumnSelectionMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component
public class ColumnSelectionMgrImpl implements ColumnSelectionMgr {

    @Resource(name = "externalColumnEntityMgr")
    private MetadataColumnEntityMgr<ExternalColumn> externalColumnEntityMgr;

    @Override
    public ColumnSelection getPredefined(Predefined predefined, String dataCloudVersion) {
        List<ExternalColumn> externalColumns = externalColumnEntityMgr
                .findByTag(predefined.getName(), dataCloudVersion);
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setName(predefined.getName());
        columnSelection.setVersion(getCurrentVersionOfPredefined(predefined));

        List<Column> columns = new ArrayList<>();
        for (ExternalColumn externalColumn : externalColumns) {
            if (externalColumn.getTagList().contains(predefined.getName())) {
                Column column = new Column();
                column.setExternalColumnId(externalColumn.getExternalColumnID());
                column.setColumnName(externalColumn.getDefaultColumnName());
                columns.add(column);
            }
        }
        columnSelection.setColumns(columns);
        return null;
    }

    @Override
    public ColumnSelection getPredefinedAtVersion(Predefined predefined, String version) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public String getCurrentVersionOfPredefined(Predefined predefined) {
        return "1.0";
    }

}
