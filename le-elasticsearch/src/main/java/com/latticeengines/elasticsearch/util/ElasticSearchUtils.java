package com.latticeengines.elasticsearch.util;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;

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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.elasticsearch.config.ElasticSearchConfig;

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
            log.error("index doesn't exists. or docs is empty. indexName is {}, docs: {}", indexName, docs);
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

    public static void createIndex(RestHighLevelClient client, String indexName, XContentBuilder builder) throws IOException {
        log.info("builder = {}", builder);
        if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.mapping(builder);

            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            log.info("Index {} created, response = {}", indexName, response);
        }
    }

    public static void createIndexWithSettings(RestHighLevelClient client, String indexName,
                                               ElasticSearchConfig esConfig,
                                               XContentBuilder builder) throws IOException {
        log.info("builder = {}", builder);
        if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder() //
                    .put("index.number_of_shards", esConfig.getShards()) //
                    .put("index.number_of_replicas", esConfig.getReplicas()) //
                    .put("index.refresh_interval", "60s"));
            request.mapping(builder);

            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            log.info("Index {} created, response = {}", indexName, response);
        }
    }

    public static XContentBuilder initIndexMapping(boolean dynamic) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder() //
                .startObject() //
                .field("dynamic", dynamic) //
                .startObject("properties");
        return builder
                    .startObject(WebVisitDate.name())
                    .field("type", "date")
                    .endObject().endObject().endObject();
    }
}
