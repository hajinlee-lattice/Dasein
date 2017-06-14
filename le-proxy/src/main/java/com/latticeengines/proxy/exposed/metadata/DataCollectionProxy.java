package com.latticeengines.proxy.exposed.metadata;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.network.exposed.metadata.DataCollectionInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionProxy")
public class DataCollectionProxy extends MicroserviceRestApiProxy implements DataCollectionInterface {

    protected DataCollectionProxy() {
        super("metadata");
    }

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections", customerSpace);
        List<?> list = get("getDataCollections", url, List.class);
        return JsonUtils.convertList(list, DataCollection.class);
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/types/{type}", customerSpace, type);
        return get("getDataCollection", url, DataCollection.class);
    }

    @Override
    public DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections", customerSpace);
        return post("createOrUpdateDataCollection", url, dataCollection, DataCollection.class);
    }

    @Override
    public DataCollection upsertTableToDataCollection(String customerSpace, DataCollectionType dataCollectionType,
            String tableName, boolean purgeOldTable) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollections/types/{dataCollectionType}/tables/{tableName}?purgeOld={purgeOldTable}",
                customerSpace, dataCollectionType, tableName, purgeOldTable);
        return post("upsertTableToDataCollection", url, null, DataCollection.class);
    }

    @Override
    public DataCollection upsertStatsToDataCollection(String customerSpace, DataCollectionType dataCollectionType,
            StatisticsContainer container, boolean purgeOld) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollections/types/{dataCollectionType}/stats?purgeOld={purgeOld}",
                customerSpace, dataCollectionType, purgeOld);
        return post("upsertStatsToDataCollection", url, container, DataCollection.class);
    }

}
