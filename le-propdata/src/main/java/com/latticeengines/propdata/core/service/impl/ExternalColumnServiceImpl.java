package com.latticeengines.propdata.core.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.core.entitymgr.ExternalColumnEntityMgr;
import com.latticeengines.propdata.core.service.ExternalColumnService;

@Component("externalColumnService")
public class ExternalColumnServiceImpl implements ExternalColumnService {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    public List<ExternalColumn> columnSelection(ColumnSelection.Predefined selectName) {
        switch (selectName) {
        case LEAD_ENRICHMENT:
            return externalColumnEntityMgr.getLeadEnrichment();
        default:
            throw new LedpException(LedpCode.LEDP_25005, new String[] { String.valueOf(selectName) });
        }
    }
}
