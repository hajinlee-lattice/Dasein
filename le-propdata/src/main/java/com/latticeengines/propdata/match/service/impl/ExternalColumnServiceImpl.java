package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;
import com.latticeengines.propdata.match.service.ExternalColumnService;

@Component("externalColumnService")
public class ExternalColumnServiceImpl implements ExternalColumnService {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    public List<ExternalColumn> columnSelection(ColumnSelection.Predefined selectName) {
        switch (selectName) {
            case LeadEnrichment:
            case DerivedColumns:
            case Model:
                return externalColumnEntityMgr.findByTag(selectName.getName());
            default:
                throw new LedpException(LedpCode.LEDP_25005, new String[] { String.valueOf(selectName) });
        }
    }
}
