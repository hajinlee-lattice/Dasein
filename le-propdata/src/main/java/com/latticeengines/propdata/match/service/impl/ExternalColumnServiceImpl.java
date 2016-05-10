package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;
import com.latticeengines.propdata.match.service.ExternalColumnService;

@Component("externalColumnService")
public class ExternalColumnServiceImpl implements ExternalColumnService {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    public List<ExternalColumn> columnSelection(ColumnSelection.Predefined selectName) {
        return externalColumnEntityMgr.findByTag(selectName.getName());
    }
}
