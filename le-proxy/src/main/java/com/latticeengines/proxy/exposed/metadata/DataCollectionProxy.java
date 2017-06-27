package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
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

    // default collection apis

    public DataCollection getDefaultDataCollection(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection",
                shortenCustomerSpace(customerSpace));
        return get("get default dataCollection", url, DataCollection.class);
    }

    public StatisticsContainer getStatsInDefaultColellction(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        return get("get stats", url, StatisticsContainer.class);
    }

    public Table getTableInDefaultCollection(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/tables?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        return get("getTable", url, Table.class);
    }

    // full collection apis

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
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/stats",
                shortenCustomerSpace(customerSpace), collectionName);
        post("upsertStats", url, container, SimpleBooleanResponse.class);
    }

    public StatisticsContainer getStats(String customerSpace, String collectionName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/{dataCollectionName}/stats",
                shortenCustomerSpace(customerSpace), collectionName);
        return get("getStats", url, StatisticsContainer.class);
    }

}
