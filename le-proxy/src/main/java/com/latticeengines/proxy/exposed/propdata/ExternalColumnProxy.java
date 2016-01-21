package com.latticeengines.proxy.exposed.propdata;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.network.exposed.propdata.ExternalColumnInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class ExternalColumnProxy extends BaseRestApiProxy implements ExternalColumnInterface{
	
	public ExternalColumnProxy() {
		super("propdata/metadata");
	}
	
	@SuppressWarnings("unchecked")
	@Override
    public List<ColumnMetadata> getLeadEnrichment() {
		String url = constructUrl("/predefined/leadenrichment");
        return get("getLeadEnrichment", url, List.class);
	}

}
