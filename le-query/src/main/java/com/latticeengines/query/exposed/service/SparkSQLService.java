package com.latticeengines.query.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.spark.LivySession;

public interface SparkSQLService {

    LivySession initializeLivySession(AttributeRepository attrRepo, Map<String, String> hdfsPathMap, //
                                      int scalingFactor, boolean persist, String secondaryJobName);

    long getCount(CustomerSpace customerSpace, LivySession livySession, String sql);

    HdfsDataUnit getData(CustomerSpace customerSpace, LivySession livySession, String sql, //
                         Map<String, Map<Long, String>> decodeMapping);

}
