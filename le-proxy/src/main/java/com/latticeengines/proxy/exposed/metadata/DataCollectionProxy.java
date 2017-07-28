package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
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

    public AttributeRepository getDefaultAttributeRepository(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/attrrepo",
                shortenCustomerSpace(customerSpace));
        return get("get default attributeRepository", url, AttributeRepository.class);
    }

    public StatisticsContainer getStats(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        return get("get stats", url, StatisticsContainer.class);
    }

    public Table getTable(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/tables?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        return get("getTable", url, Table.class);
    }

    public void resetTable(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/reset?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        post("resetTable", url, null, Table.class);
    }

    public void upsertTable(String customerSpace, String tableName, TableRoleInCollection role) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/tables/{tableName}?role={role}",
                shortenCustomerSpace(customerSpace), tableName, role);
        post("upsertTable", url, null, DataCollection.class);
    }

    public void upsertStats(String customerSpace, StatisticsContainer container) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        post("upsertStats", url, container, SimpleBooleanResponse.class);
    }

    // full collection apis
    @Deprecated
    public List<Table> getAllTables(String customerSpace, String collectionName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/{collectionName}/tables",
                shortenCustomerSpace(customerSpace), collectionName);
        List<?> list = get("getAllTables", url, List.class);
        return JsonUtils.convertList(list, Table.class);
    }
}
