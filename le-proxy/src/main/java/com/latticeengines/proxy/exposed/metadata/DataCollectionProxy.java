package com.latticeengines.proxy.exposed.metadata;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionProxy")
public class DataCollectionProxy extends MicroserviceRestApiProxy {

    protected DataCollectionProxy() {
        super("metadata");
    }

    public List<DataCollection> getDataCollections(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections",
                shortenCustomerSpace(customerSpace));
        List<?> list = get("getDataCollections", url, List.class);
        return JsonUtils.convertList(list, DataCollection.class);
    }

    public List<Table> getAllTables(String customerSpace, String collectionName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/{collectionName}/tables",
                shortenCustomerSpace(customerSpace), collectionName);
        List<?> list = get("getAllTables", url, List.class);
        return JsonUtils.convertList(list, Table.class);
    }

    public Table getTable(String customerSpace, String collectionName, TableRoleInCollection tableRole) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollections/{collectionName}/tables?role={tableRole}", //
                shortenCustomerSpace(customerSpace), collectionName, tableRole);
        List<?> list = get("getTable", url, List.class);
        if (list == null || list.isEmpty()) {
            return null;
        } else {
            return JsonUtils.convertList(list, Table.class).get(0);
        }
    }

    public DataCollection getDataCollection(String customerSpace, String collectionName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/{collectionName}",
                shortenCustomerSpace(customerSpace), collectionName);
        return get("getDataCollection", url, DataCollection.class);
    }

    @Deprecated
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/types/{type}",
                shortenCustomerSpace(customerSpace), type);
        return get("getDataCollection", url, DataCollection.class);
    }

    public DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections",
                shortenCustomerSpace(customerSpace));
        return post("createOrUpdateDataCollection", url, dataCollection, DataCollection.class);
    }

    public void addDataFeed(String customerSpace, String collectionName, DataFeed dataFeed) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/datafeeds",
                shortenCustomerSpace(customerSpace), collectionName);
        post("add data feed", url, dataFeed, DataCollection.class);
    }

    public void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection role) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/tables/{tableName}?role={role}",
                shortenCustomerSpace(customerSpace), collectionName, tableName, role);
        post("upsertTable", url, null, DataCollection.class);
    }

    public void upsertStats(String customerSpace, String collectionName, StatisticsContainer container) {
        upsertStatsForModel(customerSpace, collectionName, container, null);
    }

    public void upsertStatsForModel(String customerSpace, String collectionName, StatisticsContainer container,
            String modelId) {
        String url;
        if (StringUtils.isBlank(modelId)) {
            url = constructUrl("/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/stats",
                    shortenCustomerSpace(customerSpace), collectionName);
        } else {
            url = constructUrl(
                    "/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/stats?model={modelId}",
                    shortenCustomerSpace(customerSpace), collectionName, modelId);
        }
        post("upsertStats", url, container, SimpleBooleanResponse.class);
    }

    public StatisticsContainer getStats(String customerSpace, String collectionName) {
        return getStatsForModel(customerSpace, collectionName, null);
    }

    public StatisticsContainer getStatsForModel(String customerSpace, String collectionName, String modelId) {
        String url;
        if (StringUtils.isBlank(modelId)) {
            url = constructUrl("/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/stats",
                    shortenCustomerSpace(customerSpace), collectionName);
        } else {
            url = constructUrl(
                    "/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/stats?model={modelId}",
                    shortenCustomerSpace(customerSpace), collectionName, modelId);
        }
        return get("getStats", url, StatisticsContainer.class);
    }

    private String shortenCustomerSpace(String customerSpace) {
        return CustomerSpace.parse(customerSpace).getTenantId();
    }

}
