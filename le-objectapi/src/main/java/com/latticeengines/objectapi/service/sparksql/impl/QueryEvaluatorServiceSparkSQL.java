package com.latticeengines.objectapi.service.sparksql.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema.Field;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.service.SparkSQLService;

import reactor.core.publisher.Flux;

@Component("queryEvaluatorServiceSparkSQL")
public class QueryEvaluatorServiceSparkSQL extends QueryEvaluatorService {

    private static final Logger log = LoggerFactory.getLogger(QueryEvaluatorServiceSparkSQL.class);

    @Inject
    private SparkSQLService sparkSQLService;

    @Inject
    protected Configuration yarnConfiguration;

    private LivySession livySession;

    public QueryEvaluatorServiceSparkSQL() {
        log.info("Created QueryEvaluatorService with SparkSession");
    }

    public void setLivySession(LivySession livySession) {
        if (livySession != null) {
            log.info("Link " + getClass().getSimpleName() + " to livy session " //
                    + livySession.getSessionId() + "[" + livySession.getAppId() + "]");
        } else if (this.livySession != null){
            log.info("Detach " + getClass().getSimpleName() + " from livy session " //
                    + this.livySession.getSessionId() + "[" + this.livySession.getAppId() + "]");
        }
        this.livySession = livySession;
    }

    @Override
    public long getCount(AttributeRepository attrRepo, Query query, String sqlUser) {
        String sql = super.getQueryStr(attrRepo, query, sqlUser);
        return sparkSQLService.getCount(attrRepo.getCustomerSpace(), livySession, sql);
    }

    @Override
    public DataPage getData(String customerSpace, Version version, Query query, String sqlUser) {
        return super.getData(customerSpace, version, query, sqlUser);
    }

    @Override
    public DataPage getData(AttributeRepository attrRepo, Query query, String sqlUser) {
        List<Map<String, Object>> resultData = getDataFlux(attrRepo, query, sqlUser).collectList().block();
        return new DataPage(resultData);
    }

    @Override
    public Flux<Map<String, Object>> getDataFlux(AttributeRepository attrRepo, Query query, String sqlUser, //
                                                 Map<String, Map<Long, String>> decodeMapping) {
        String sql = super.getQueryStr(attrRepo, query, sqlUser);
        HdfsDataUnit hdfsDataUnit = //
                sparkSQLService.getData(attrRepo.getCustomerSpace(), livySession, sql, decodeMapping);
        List<Map<String, Object>> resultData = convertHdfsDataUnitToList(hdfsDataUnit);
        return Flux.fromIterable(resultData);
    }

    private List<Map<String, Object>> convertHdfsDataUnitToList(HdfsDataUnit sparkResult) {
        List<Map<String, Object>> resultData = new ArrayList<>();
        String avroPath = sparkResult.getPath();
        AvroUtils.AvroFilesIterator iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, avroPath + "/*.avro");
        iterator.forEachRemaining(record -> {
            Map<String, Object> row = new HashMap<>();
            for (Field field: record.getSchema().getFields()) {
                Object value = record.get(field.name());
                if (value instanceof Utf8) {
                    value = ((Utf8)value).toString();
                }
                row.put(field.name(), value);
            }
            resultData.add(row);
        });
        return resultData;
    }

    List<String> createViews(CustomerSpace customerSpace, List<Pair<String, String>> views) {
        return sparkSQLService.createViews(customerSpace, livySession, views);
    }

    HdfsDataUnit mergeRules(CustomerSpace customerSpace, List<String> bucketViews, List<String> tempViews, //
                            String defaultBucket) {
        return sparkSQLService.mergeRules(customerSpace, livySession, bucketViews, tempViews, defaultBucket);
    }

    Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

}
