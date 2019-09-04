package com.latticeengines.query.exposed.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.spark.LivySession;

public interface SparkSQLService {

    LivySession initializeLivySession(AttributeRepository attrRepo, Map<String, String> hdfsPathMap, //
                                      int scalingFactor, String storageLevel, String secondaryJobName);

    void prepareForCrossSellQueries(LivySession livySession, String period, String trxnTable, String storageLevel);

    long getCount(CustomerSpace customerSpace, LivySession livySession, String sql);

    HdfsDataUnit getData(CustomerSpace customerSpace, LivySession livySession, String sql, //
                         Map<String, Map<Long, String>> decodeMapping);

    HdfsDataUnit mergeRules(CustomerSpace customerSpace, LivySession livySession, //
                            List<String> bktViewList, List<String> tempViewList, String defaultBucketName);

    List<String> createViews(CustomerSpace customerSpace, LivySession livySession, List<Pair<String, String>> views);

}
