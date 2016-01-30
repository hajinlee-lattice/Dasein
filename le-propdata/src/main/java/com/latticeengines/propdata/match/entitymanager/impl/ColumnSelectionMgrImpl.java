package com.latticeengines.propdata.match.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.entitymanager.ColumnSelectionMgr;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;

@Component
public class ColumnSelectionMgrImpl implements ColumnSelectionMgr {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    @Override
    public ColumnSelection getPredefined(ColumnSelection.Predefined predefined) {
        List<ExternalColumn> externalColumns = externalColumnEntityMgr.findByTag(predefined.getName());
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setName(predefined.getName());
        columnSelection.setVersion(getCurrentVersionOfPredefined(predefined));

        List<ColumnSelection.Column> columns = new ArrayList<>();
        for (ExternalColumn externalColumn: externalColumns) {
            ColumnSelection.Column column = new ColumnSelection.Column();
            column.setExternalColumnId(externalColumn.getExternalColumnID());
            column.setColumnName(externalColumn.getDefaultColumnName());
            columns.add(column);
        }
        columnSelection.setColumns(columns);
        return null;
    }

    @Override
    public ColumnSelection getPredefinedAtVersion(ColumnSelection.Predefined predefined, String version) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public String getCurrentVersionOfPredefined(ColumnSelection.Predefined predefined) {
        return "1.0";
    }

}
