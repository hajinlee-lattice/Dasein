package com.latticeengines.propdata.match.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.Column;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;
import com.latticeengines.propdata.match.entitymanager.ColumnSelectionMgr;
import com.latticeengines.propdata.match.entitymanager.MetadataColumnEntityMgr;

@Component
public class ColumnSelectionMgrImpl implements ColumnSelectionMgr {

    @Resource(name = "externalColumnEntityMgr")
    private MetadataColumnEntityMgr<ExternalColumn> externalColumnEntityMgr;

    @Override
    public ColumnSelection getPredefined(Predefined predefined) {
        List<ExternalColumn> externalColumns = externalColumnEntityMgr.findByTag(predefined.getName());
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
