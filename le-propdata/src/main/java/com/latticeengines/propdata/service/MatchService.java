package com.latticeengines.propdata.service;

import java.util.List;
import java.util.Map;

public interface MatchService {


	Map<String, Map<String, Object>> match(String domain, List<String> sources);

	void createDomainIndex(String sourceName);

}
