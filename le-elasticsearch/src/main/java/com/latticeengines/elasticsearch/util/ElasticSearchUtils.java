package com.latticeengines.elasticsearch.util;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.EventDate;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

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

    public static XContentBuilder initIndexMapping(String entityType, boolean dynamic) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder() //
                .startObject() //
                .field("dynamic", dynamic) //
                .startObject("properties");
        switch (entityType) {
            case "WebVisitProfile": return builder
                    .startObject(WebVisitDate.name())
                    .field("type", "date")
                    .endObject().endObject().endObject();
            case "Contact": return builder.startObject(AccountId.name()) //
                    .field("type", "keyword") //
                    .endObject().endObject().endObject();
            case "TimelineProfile": return builder.startObject(AccountId.name()) //
                    .field("type", "keyword") //
                    .endObject() //
                    .startObject(ContactId.name()) //
                    .field("type", "keyword") //
                    .endObject() //
                    .startObject(EventDate.getColumnName()) //
                    .field("type", "date") //
                    .endObject().endObject().endObject();
            default: return null;
        }
    }

    public static XContentBuilder initAccountIndexMapping(boolean dynamic, List<String> lookupIds) throws IOException {
        XContentBuilder accountBuilder = XContentFactory.jsonBuilder() //
                .startObject() //
                .field("dynamic", dynamic) //
                .startObject("properties");

        if (lookupIds != null) {
            for (String lookupId : lookupIds) {
                accountBuilder //
                        .startObject(BucketedAccount.name() + ":" + lookupId) //
                        .field("type", "keyword") //
                        .endObject();
            }
        }
        accountBuilder.endObject().endObject();
        return accountBuilder;
    }

    public static GetResponse searchByAccountId(RestHighLevelClient client, String indexName, String accountId) {
        return searchByEntityId(client, indexName, accountId);
    }

    public static GetResponse searchByEntityId(RestHighLevelClient client, String indexName, String entityId) {
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("searchByEntityId",  start)) {
            workflowSpan = tracer.activeSpan();
            GetRequest getRequest = new GetRequest(indexName, entityId);
            workflowSpan.log(String.format("indexName : %s, search entity query : %s", indexName,
                    getRequest.toString()));
            try {
                return client.get(getRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("can't find search result, entityId is {}, indexName is {}, due to error: {}", entityId,
                        indexName, e.getMessage());
                return null;
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.NOT_FOUND) {
                    log.info("can't find index {}.", indexName);
                } else {
                    log.error("can't find search result, entityId is {}, indexName is {}, due to error: {}", entityId,
                            indexName, e.getMessage());
                }
                return null;
            }
        } finally {
            finish(workflowSpan);
        }
    }

    public static SearchResponse searchContactByAccountId(RestHighLevelClient client, String indexName,
                                                          String accountId) {
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("searchContactByAccountId",  start)) {
            workflowSpan = tracer.activeSpan();
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            TermQueryBuilder queryBuilder = QueryBuilders.termQuery(AccountId.name(), accountId);
            searchSourceBuilder.query(queryBuilder);
            workflowSpan.log(String.format("indexName : %s, contact query : %s", indexName, queryBuilder.toString()));
            log.info("contact query is {}.", searchSourceBuilder);
            request.source(searchSourceBuilder);
            try {
                SearchResponse response = client.search(request, RequestOptions.DEFAULT);
                log.info("response is {}.", JsonUtils.serialize(response));
                return response;
            } catch (IOException e) {
                log.error("can't find search result, due to error: {}", e.getMessage());
                return null;
            }
        } finally {
            finish(workflowSpan);
        }
    }

    public static GetResponse searchContactByContactId(RestHighLevelClient client, String indexName,
                                                       String contactId) {
        return searchByEntityId(client, indexName, contactId);
    }

    public static SearchResponse searchTimelineByEntityIdAndDateRange(RestHighLevelClient client, String indexName,
                                                                      String entity, String entityId, Long fromDate,
                                                                      Long toDate) {

        String entityIdColumn = BusinessEntity.Account.name().equalsIgnoreCase(entity) ? AccountId.name()
                : ContactId.name();
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("searchTimelineByEntityIdAndDateRange",  start)) {
            workflowSpan = tracer.activeSpan();
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder queryBuilder = QueryBuilders //
                    .boolQuery() //
                    .must(QueryBuilders.termQuery(entityIdColumn, entityId)) //
                    .must(QueryBuilders.rangeQuery(EventDate.getColumnName()).gte(fromDate).lte(toDate));
            log.info("timeline query is {}.", queryBuilder);
            workflowSpan.log(String.format("indexName : %s, timeline query : %s", indexName, queryBuilder.toString()));
            searchSourceBuilder.query(queryBuilder);
            request.source(searchSourceBuilder);
            try {
                SearchResponse response = client.search(request, RequestOptions.DEFAULT);
                log.info("response is {}.", JsonUtils.serialize(response));
                return response;
            } catch (IOException e) {
                log.error("can't find search result, due to error: {}", e.getMessage());
                return null;
            }
        } finally {
            finish(workflowSpan);
        }
    }

    public static SearchResponse searchByLookupId(RestHighLevelClient client, String indexName, String lookupIdKey,
                                                  String lookupIdValue) {
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("searchByLookupId",  start)) {
            workflowSpan = tracer.activeSpan();
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            TermQueryBuilder queryBuilder = QueryBuilders.termQuery(
                    TableRoleInCollection.BucketedAccount.ordinal() + ":" + lookupIdKey, lookupIdValue.toLowerCase());
            searchSourceBuilder.query(queryBuilder);
            log.info("accountByLookup query is {}.", searchSourceBuilder);
            workflowSpan.log(String.format("indexName : %s, accountByLookup query : %s",
                    indexName, queryBuilder.toString()));
            searchRequest.source(searchSourceBuilder);
            try {
                SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                log.info("response is {}.", JsonUtils.serialize(response));
                return response;
            } catch (IOException e) {
                log.error("can't find search result, due to error: {}", e.getMessage());
                return null;
            }
        } finally {
            finish(workflowSpan);
        }
    }

    public static String searchAccountIdByLookupId(RestHighLevelClient client, String indexName, String lookupIdKey,
                                                   String lookupIdValue) {
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("searchAccountIdByLookupId",  start)) {
            workflowSpan = tracer.activeSpan();
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            TermQueryBuilder queryBuilder = QueryBuilders.termQuery(
                    TableRoleInCollection.BucketedAccount.ordinal() + ":" + lookupIdKey, lookupIdValue.toLowerCase());
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.fetchSource(false);
            searchSourceBuilder.fetchSource(AccountId.name(), null);
            workflowSpan.log(String.format("indexName : %s, search account by lookupIdquery : %s", indexName,
                    queryBuilder.toString()));
            request.source(searchSourceBuilder);
            try {
                SearchResponse response = client.search(request, RequestOptions.DEFAULT);
                SearchHits hits = response.getHits();
                SearchHit[] searchHits = hits.getHits();
                for (SearchHit hit : searchHits) {
                    return hit.getId();//get docId
                }
            } catch (IOException e) {
                log.error("can't find search result, due to error: {}", e.getMessage());
                return null;
            }
            return null;
        } finally {
            finish(workflowSpan);
        }
    }

    public static Scope startSpan(String methodName, long startTime) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("ElasticSearchUtils - " + methodName) //
                .asChildOf(tracer.activeSpan())
                .withStartTimestamp(startTime) //
                .start();
        return tracer.activateSpan(span);
    }

    public static void finish(Span span) {
        if (span != null) {
            span.finish();
        }
    }
}
