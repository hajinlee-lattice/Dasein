package com.latticeengines.proxy.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;


@Component("dataUnitProxy")
public class DataUnitProxyImpl extends MicroserviceRestApiProxy implements DataUnitProxy {

    protected DataUnitProxyImpl() {
        super("metadata");
    }

    @Override
    public DataUnit create(String customerSpace, DataUnit dataUnit) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit", shortenCustomerSpace(customerSpace));
        return post("create data unit", url, dataUnit, DataUnit.class);
    }

    @Override
    public List<DataUnit> getByStorageType(String customerSpace, DataUnit.StorageType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/type/{type}",
                shortenCustomerSpace(customerSpace), type.name());
        List<?> list = get("get data units", url, List.class);
        return JsonUtils.convertList(list, DataUnit.class);
    }

    @Override
    public DataUnit getByNameAndType(String customerSpace, String name, DataUnit.StorageType type) {
        List<DataUnit> units = getDataUnits(customerSpace, name, type);
        if (CollectionUtils.isNotEmpty(units)) {
            return units.get(0);
        } else {
            return null;
        }
    }

    private List<DataUnit> getDataUnits(String customerSpace, String name, DataUnit.StorageType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/name/{name}", shortenCustomerSpace(customerSpace), name);
        if (type != null) {
            url += "?type=" + type.name();
        }
        List<?> list = get("get data units", url, List.class);
        return JsonUtils.convertList(list, DataUnit.class);
    }
}
