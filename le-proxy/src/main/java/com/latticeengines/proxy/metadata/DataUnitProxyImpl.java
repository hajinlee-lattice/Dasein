package com.latticeengines.proxy.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
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
    public DataUnit create(String customerSpace, DataUnit dataUnit, boolean purgeOldSnapShot) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit", shortenCustomerSpace(customerSpace));
        url += "?" + "purgeOldSnapShot=" + purgeOldSnapShot;
        return post("create data unit", url, dataUnit, DataUnit.class);
    }

    @Override
    public DataUnit updateByNameAndType(String customerSpace, DataUnit dataUnit) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit", shortenCustomerSpace(customerSpace));
        return put("update data unit", url, dataUnit, DataUnit.class);
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

    @Override
    public DataUnit getByDataTemplateIdAndRole(String customerSpace, String dataTemplateId, DataUnit.Role role) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/template/{templateId}", shortenCustomerSpace(customerSpace), dataTemplateId);
        url += "?role=" + role.name();
        return get("get data unit by template id and role", url, DataUnit.class);
    }

    @Override
    public Boolean renameTableName(String customerSpace, DataUnit dataUnit, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/renameTableName?tableName" +
                        "={tableName}",
                shortenCustomerSpace(customerSpace), tableName);
        return post("Rename RedShift tableName", url, dataUnit, Boolean.class);
    }

    @Override
    public Boolean delete(String customerSpace, DataUnit dataUnit) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/delete",
                shortenCustomerSpace(customerSpace));
        return put("delete DataUnit", url, dataUnit, Boolean.class);
    }

    @Override
    public Boolean delete(String customerSpace, String name, DataUnit.StorageType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/delete/name/{name}", shortenCustomerSpace(customerSpace), name);
        if (type != null) {
            url += "?type=" + type.name();
        }
        delete("delete data unit by name and type", url);
        return true;
    }

    @Override
    public void updateSignature(String customerSpace, DataUnit dataUnit, String signature) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/updateSignature?signature={signature}",
                shortenCustomerSpace(customerSpace), signature);
        post("updateSignature", url, dataUnit, DataUnit.class);
    }

    private List<DataUnit> getDataUnits(String customerSpace, String name, DataUnit.StorageType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/name/{name}", shortenCustomerSpace(customerSpace), name);
        if (type != null) {
            url += "?type=" + type.name();
        }
        List<?> list = get("get data units", url, List.class);
        return JsonUtils.convertList(list, DataUnit.class);
    }

    @Override
    public AthenaDataUnit registerAthenaDataUnit(String customerSpace, String name) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit/name/{name}/athena-unit", customerSpace, name);
        return post("register athena data unit", url, null, AthenaDataUnit.class);
    }
}
