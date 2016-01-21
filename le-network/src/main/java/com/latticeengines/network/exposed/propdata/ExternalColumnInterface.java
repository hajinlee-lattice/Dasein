package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;

public interface ExternalColumnInterface {
	List<ColumnMetadata> getLeadEnrichment();
}
