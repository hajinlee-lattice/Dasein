package com.latticeengines.elasticsearch.util;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountLookup;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.EventDate;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

public final class ElasticSearchUtils {

    private static Logger log = LoggerFactory.getLogger(ElasticSearchUtils.class);

    private static final String PROPERTIES = "properties";

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
        if (!indexExists(client, indexName)) {
            return;
        }
        IndexRequest indexRequest = new IndexRequest(indexName, null, docId);
        indexRequest.source(jsonString, XContentType.JSON);
        log.info("indexrequest: {}.", indexRequest.toString());
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info(indexResponse.toString());
    }

    public static void createDocuments(RestHighLevelClient client, String indexName, Map<String, String> docs) throws IOException {
        if (!indexExists(client, indexName) || MapUtils.isEmpty(docs)) {
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
        if (!indexExists(client, indexName)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.mapping(builder);

            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            log.info("Index {} created, response = {}", indexName, response);
        }
    }

    /**
     * check one index exists
     * @param client
     * @param indexName
     * @return
     * @throws IOException
     */
    public static boolean indexExists(RestHighLevelClient client, String indexName) throws IOException {
        return client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
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
                    .put("index.refresh_interval", "60s")
                    .loadFromSource(Strings.toString(XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("analysis")
                            .startObject("normalizer")
                            .startObject("lowercase_normalizer")
                            .field("type", "custom")
                            .field("filter", new String[]{"lowercase"})
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()), XContentType.JSON));
            request.mapping(builder);

            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            log.info("Index {} created, response = {}", indexName, response);
        }
    }


    /**
     * check one field name exists in the index
     * @param client
     * @param indexName
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static boolean checkFieldExist(RestHighLevelClient client, String indexName, String fieldName) throws IOException {
        if (!indexExists(client, indexName)) {
            return false;
        }
        Map<String, Object> map = getSourceMapping(client, indexName);
        if (map != null) {
            return map.containsKey(fieldName);
        }
        return false;
    }

    /**
     * return the source mapping in index
     * @param client
     * @param indexName
     * @return
     * @throws IOException
     */
    public static Map<String, Object> getSourceMapping(RestHighLevelClient client,
                                                       String indexName) throws IOException{
        if (!indexExists(client, indexName)) {
            return null;
        }
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(indexName);
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> mappings = response.mappings();
        MappingMetadata metadata = mappings.get(indexName);
        if (metadata != null) {
            Map<String, Object> source = metadata.getSourceAsMap();
            if (source != null) {
                Object sourceMap = source.get(PROPERTIES);
                if (sourceMap != null) {
                    return JsonUtils.convertMap(JsonUtils.convertValue(sourceMap, Map.class), String.class,
                            Object.class);
                }
            }
        }
        return null;
    }

    /**
     * update the mapping if the field name is not set in index
     * @param client
     * @param indexName
     * @param fieldName
     * @throws IOException
     */
    public static boolean updateIndexMapping(RestHighLevelClient client, String indexName,
                                  String fieldName, String type) throws IOException {
        if (checkFieldExist(client, indexName, fieldName)) {
            log.info("field {} exists in the index {}", fieldName, indexName);
            return false;
        }
        log.info("set field name {} with type {} for index {}", fieldName, type, indexName);
        PutMappingRequest request = new PutMappingRequest(indexName);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject(PROPERTIES)
                .startObject(fieldName)
                .field("type", type)
        .endObject().endObject().endObject();
        request.source(builder);

        client.indices().putMapping(request, RequestOptions.DEFAULT);
        return true;
    }

    /**
     * update nested mapping for account
     * @param client
     * @param indexName
     * @param fieldName
     * @param type
     * @param lookupIds
     * @return
     * @throws IOException
     */
    public static boolean updateAccountIndexMapping(RestHighLevelClient client, String indexName, String fieldName,
                                                    String type, List<String> lookupIds, String subType) throws IOException {
        if (checkFieldExist(client, indexName, fieldName)) {
            log.info("field {} exists in the index {}", fieldName, indexName);
            return false;
        }
        log.info("set field name {} with type {} for index {}", fieldName, type, indexName);

        PutMappingRequest request = new PutMappingRequest(indexName);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject(PROPERTIES)
                .startObject(fieldName)
                .field("type", type)
                .startObject(PROPERTIES);
        if (CollectionUtils.isNotEmpty(lookupIds)) {
            for (String lookupId : lookupIds)
                builder.startObject(lookupId)
                        .field("type", subType)
                        .field("normalizer", "lowercase_normalizer")
                        .endObject();
        }
        builder.endObject().endObject().endObject().endObject();
        request.source(builder);

        client.indices().putMapping(request, RequestOptions.DEFAULT);
        return true;
    }

    /**
     * delete one index
     * @param client
     * @param indexName
     * @throws IOException
     */
    public static boolean deleteIndex(RestHighLevelClient client, String indexName) throws IOException {
        if (!indexExists(client, indexName)) {
            log.info("index {} doesn't exist", indexName);
            return false;
        }
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        client.indices().delete(request, RequestOptions.DEFAULT);
        return true;
    }

    public static XContentBuilder initIndexMapping(String entityType, boolean dynamic) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder() //
                .startObject() //
                .field("dynamic", dynamic) //
                .startObject(PROPERTIES);
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
                .startObject(PROPERTIES);

        if (lookupIds != null) {
            for (String lookupId : lookupIds) {
                accountBuilder //
                        .startObject(lookupId) //
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
                    AccountLookup.name() + "." + lookupIdKey, lookupIdValue);
            NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(AccountLookup.name(), queryBuilder,
                    ScoreMode.Max);
            searchSourceBuilder.query(nestedQueryBuilder);
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
            TermQueryBuilder queryBuilder = QueryBuilders.termQuery(AccountLookup.name() + "." + lookupIdKey,
                    lookupIdValue);
            NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(AccountLookup.name(), queryBuilder,
                    ScoreMode.Max);
            searchSourceBuilder.query(nestedQueryBuilder);
            searchSourceBuilder.fetchSource(false);
           // searchSourceBuilder.fetchSource(AccountId.name(), null);
            workflowSpan.log(String.format("indexName : %s, search account by lookupIdquery : %s", indexName,
                    nestedQueryBuilder.toString()));
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

    public static String getEntityFromTableRole(TableRoleInCollection tableRoleInCollection) {
        switch(tableRoleInCollection) {
            case AccountLookup:
            case ConsolidatedAccount:
            case CalculatedCuratedAccountAttribute:
            case CalculatedPurchaseHistory:
            case PivotedRating:
            case WebVisitProfile:
            case OpportunityProfile:
            case AccountMarketingActivityProfile:
            case CustomIntentProfile:
                return BusinessEntity.Account.name();
            case ConsolidatedContact:
            case CalculatedCuratedContact:
                return BusinessEntity.Contact.name();
            case TimelineProfile:
                return TimelineProfile.name();
            default:
                return null;
        }
    }

    public static String generateNewVersion() {
        return String.valueOf(Instant.now().toEpochMilli());
    }

    public static String constructIndexName(String customerSpace, String entity, String signature) {
        return String.format("%s_%s_%s", CustomerSpace.shortenCustomerSpace(customerSpace),
                entity, signature).toLowerCase();
    }
}
