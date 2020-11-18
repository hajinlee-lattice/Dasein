package com.latticeengines.pls.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class ElasticSearchUtil {

    private static Logger log = LoggerFactory.getLogger(ElasticSearchUtil.class);

    protected ElasticSearchUtil() {
        throw new UnsupportedOperationException();
    }

    public static Map<String, ClusterIndexHealth> getDashboardStatus(RestHighLevelClient client) {
        ClusterHealthRequest request = new ClusterHealthRequest();
        try {
            ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
            Map<String, ClusterIndexHealth> indices = response.getIndices();
            if (MapUtils.isEmpty(indices)) {
                log.error("Can't find index in client : {}", client);
                return null;
            }
            return indices;
        } catch (IOException e) {
            log.error("Can't find es cluster. client is {}", client);
            e.printStackTrace();
            return null;
        }

    }

    public static void createDocument(RestHighLevelClient client, String indexName, String docId, String jsonString) {
        try {
            if (client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                IndexRequest indexRequest = new IndexRequest(indexName, null, docId);
                indexRequest.source(jsonString, XContentType.JSON);
                log.info("indexrequest: {}.", indexRequest.toString());
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                log.info(indexResponse.toString());
            }
        } catch (IOException e) {
            log.error(String.format("Failed to create index %s", indexName), e);
        }
    }

    public static void createIndexWithSettings(RestHighLevelClient client, String indexName,
                                               XContentBuilder builder) {

        try {
            log.info("builder = {}", builder);
            if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                CreateIndexRequest request = new CreateIndexRequest(indexName);
                request.settings(Settings.builder() //
                        .put("index.refresh_interval", "60s"));
                request.mapping(builder);

                CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
                log.info("Index {} created, response = {}", indexName, response);
            }
        } catch (Exception e) {
            log.error(String.format("Failed to create index %s", indexName), e);
        }
    }

    public static Map<String, XContentBuilder> initIndexMapping() throws IOException {
        Map<String, XContentBuilder> builderMap = new HashMap<>();
        XContentBuilder indexPatternBuilder = XContentFactory.jsonBuilder() //
                .startObject() //
                .startObject("properties")
                .startObject(KibanaType.IndexPattern.getDataType())
                .field("type", KibanaType.IndexPattern.getDataType())
                .endObject().endObject().endObject();

        XContentBuilder visualizationBuilder = XContentFactory.jsonBuilder() //
                .startObject() //
                .startObject("properties") //
                .startObject(KibanaType.Visualization.getDataType()) //
                .field("type", KibanaType.Visualization.getDataType()) //
                .endObject().endObject().endObject();
        XContentBuilder dashboardBuilder = XContentFactory.jsonBuilder() //
                .startObject() //
                .startObject("properties") //
                .startObject(KibanaType.Dashboard.getDataType()) //
                .field("type", KibanaType.Dashboard.getDataType()) //
                .endObject().endObject().endObject();
        builderMap.put(KibanaType.IndexPattern.name(), indexPatternBuilder);
        builderMap.put(BusinessEntity.Contact.name(), visualizationBuilder);
        builderMap.put(TableRoleInCollection.TimelineProfile.name(), dashboardBuilder);
        return builderMap;
    }

    public enum KibanaType {
        IndexPattern("index-pattern"), Visualization("visualization"), Dashboard("dashboard");

        private String dataType;

        KibanaType(String dataType) {
            this.dataType = dataType;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public static KibanaType getByName(String name) {
            for (KibanaType kibanaType : values()) {
                if (kibanaType.name().equalsIgnoreCase(name)) {
                    return kibanaType;
                }
            }
            throw new IllegalArgumentException(String.format("There is no entity name %s in KibanaType", name));
        }
    }
}

