package com.latticeengines.proxy.exposed.propdata;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.network.exposed.propdata.ExternalColumnInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class ExternalColumnProxy extends BaseRestApiProxy implements ExternalColumnInterface{
	
	public ExternalColumnProxy() {
		super("propdata/externalcolumn");
	}
	
	@Override
    public List<ExternalColumn> getLeadEnrichment() {
		return null;
	}

}
