package com.latticeengines.pls.util;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ElasticSearchUtils {

    private static Logger log = LoggerFactory.getLogger(ElasticSearchUtils.class);

    protected ElasticSearchUtils() {
        throw new UnsupportedOperationException();
    }

    public static Map<String, ClusterIndexHealth> getDashboardStatus(RestHighLevelClient client) throws IOException {
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        Map<String, ClusterIndexHealth> indices = response.getIndices();
        if (MapUtils.isEmpty(indices)) {
            log.error("Can't find index in client : {}", client);
            return null;
        }
        return indices;

    }

    public static void createDocument(RestHighLevelClient client, String indexName, String docId, String jsonString) throws IOException {
        if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            return;
        }
        IndexRequest indexRequest = new IndexRequest(indexName, null, docId);
        indexRequest.source(jsonString, XContentType.JSON);
        log.info("indexrequest: {}.", indexRequest.toString());
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info(indexResponse.toString());
    }
    
    public static void createDocuments(RestHighLevelClient client, String indexName, Map<String, String> docs) throws IOException {
        if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT) || MapUtils.isEmpty(docs)) {
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (Map.Entry<String, String> docEntry : docs.entrySet()) {
            IndexRequest indexRequest = new IndexRequest(indexName, null, docEntry.getKey());
            indexRequest.source(docEntry.getValue(), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info(responses.toString());
    }
}

