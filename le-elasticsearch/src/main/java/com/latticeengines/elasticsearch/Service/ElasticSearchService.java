package com.latticeengines.elasticsearch.Service;

import java.util.Map;

import com.latticeengines.domain.exposed.elasticsearch.EsEntityType;
import com.latticeengines.elasticsearch.config.ElasticSearchConfig;

public interface ElasticSearchService {

    boolean createIndex(String indexName, EsEntityType esEntityType);

    boolean createIndexWithSettings(String indexName, ElasticSearchConfig esConfig,
                                    EsEntityType esEntityType);

    boolean createDocument(String indexName, String docId, String jsonString);

    boolean createDocuments(String indexName, Map<String, String> docs);

    ElasticSearchConfig getDefaultElasticSearchConfig();
}
