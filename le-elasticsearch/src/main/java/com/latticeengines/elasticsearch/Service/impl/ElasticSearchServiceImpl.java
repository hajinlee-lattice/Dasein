package com.latticeengines.elasticsearch.Service.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.elasticsearch.EsEntityType;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.config.ElasticSearchConfig;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;

import parquet.Preconditions;

@Service("elasticSearchService")
public class ElasticSearchServiceImpl implements ElasticSearchService {

    private static Logger log = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);

    private static final int MAX_RETRY = 3;

    @Inject
    private RestHighLevelClient client;
    @Value("${elasticsearch.shards}")
    private int esShards;
    @Value("${elasticsearch.replicas}")
    private int esReplicas;
    @Value("${elasticsearch.refreshinterval}")
    private String esRefreshInterval;
    @Value("${elasticsearch.dynamic}")
    private boolean esDynamic;
    @Value("${elasticsearch.host}")
    private String esHost;
    @Value("${elasticsearch.ports}")
    private String esPorts;
    @Value("${elasticsearch.user}")
    private String user;
    @Value("${elasticsearch.pwd.encrypted}")
    private String password;

    @Override
    public boolean createIndex(String indexName, EsEntityType esEntityType) {


        return createIndexWithSettings(indexName, getDefaultElasticSearchConfig(), esEntityType);
    }

    @Override
    public boolean createIndexWithSettings(String indexName, ElasticSearchConfig esConfig,
                                           EsEntityType esEntityType) {
        Preconditions.checkNotNull(indexName, "index Name can't be null.");
        Preconditions.checkNotNull(esConfig, "esConfig can't be null");
        Preconditions.checkNotNull(esEntityType, "xContentBuilderType can't be null.");
        if (esConfig.getRefreshInterval() == null) {
            esConfig.setRefreshInterval(esRefreshInterval);
        }
        if (esConfig.getReplicas() == null) {
            esConfig.setReplicas(esReplicas);
        }
        if (esConfig.getShards() == null) {
            esConfig.setShards(esShards);
        }
        if (esConfig.getDynamic() == null) {
            esConfig.setDynamic(esDynamic);
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createIndexWithSettings(client, indexName, esConfig,
                    ElasticSearchUtils.initIndexMapping(esEntityType, esConfig.getDynamic()));
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create index %s, xContentBuilderType is %s", indexName, esEntityType), e);
            return false;
        }
        return false;
    }

    public boolean createAccountIndexWithLookupIds(String indexName, ElasticSearchConfig esConfig,
                                                   List<String> lookupIds) {
        Preconditions.checkNotNull(indexName, "index Name can't be null.");
        Preconditions.checkNotNull(esConfig, "esConfig can't be null");
        if (esConfig.getRefreshInterval() == null) {
            esConfig.setRefreshInterval(esRefreshInterval);
        }
        if (esConfig.getReplicas() == null) {
            esConfig.setReplicas(esReplicas);
        }
        if (esConfig.getShards() == null) {
            esConfig.setShards(esShards);
        }
        if (esConfig.getDynamic() == null) {
            esConfig.setDynamic(esDynamic);
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createIndexWithSettings(client, indexName, esConfig,
                    ElasticSearchUtils.initAccountIndexMapping(esConfig.getDynamic(), lookupIds));
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create account index %s", indexName), e);
            return false;
        }
        return false;
    }

    @Override
    public boolean createDocument(String indexName, String docId, String jsonString) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createDocument(client, indexName, docId, jsonString);
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create document %s", docId), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean createDocuments(String indexName, Map<String, String> docs) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createDocuments(client, indexName, docs);
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create documents, docIds is %s.", docs.keySet().toString()), e);
            return false;
        }
        return true;
    }

    @Override
    public ElasticSearchConfig getDefaultElasticSearchConfig() {
        ElasticSearchConfig elasticSearchConfig = new ElasticSearchConfig();
        elasticSearchConfig.setShards(esShards);
        elasticSearchConfig.setReplicas(esReplicas);
        elasticSearchConfig.setRefreshInterval(esRefreshInterval);
        elasticSearchConfig.setDynamic(esDynamic);
        elasticSearchConfig.setEsHost(esHost);
        elasticSearchConfig.setEsPort(esPorts);
        elasticSearchConfig.setEsUser(user);
        elasticSearchConfig.setEsPassword(password);
        return elasticSearchConfig;
    }
}
