package com.latticeengines.propdata.core.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.core.entitymgr.ExternalColumnEntityMgr;
import com.latticeengines.propdata.core.service.ExternalColumnService;

@Component("externalColumnService")
//zdd
public class ExternalColumnServiceImpl implements ExternalColumnService{
	
	@Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;
	
	public List<ExternalColumn> getLeadEnrichment() {
		return externalColumnEntityMgr.getLeadEnrichment();
	}
}
