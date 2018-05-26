package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.serviceapps.lp.ModelFieldsToAttributesRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelMetadataProxy;

@Component
public class ModelMetadataProxyImpl extends MicroserviceRestApiProxy implements ModelMetadataProxy {

    private static final String URL_PREFX = "/customerspaces/{customerSpace}/modelmetadata";

    protected ModelMetadataProxyImpl() {
        super("lp");
    }

    @Override
    public List<VdbMetadataField> getMetadata(String customerSpace, String modelGuid) {
        String url = constructUrl(URL_PREFX + "/modelid/{modelGuid}/metadata", shortenCustomerSpace(customerSpace),
                modelGuid);
        return getList("get model metadata", url, VdbMetadataField.class);
    }

    @Override
    public Table getTrainingTableFromModelId(String customerSpace, String modelGuid) {
        String url = constructUrl(URL_PREFX + "/modelid/{modelGuid}/training-table",
                shortenCustomerSpace(customerSpace), modelGuid);
        return get("get model training table", url, Table.class);
    }

    @Override
    public Table getEventTableFromModelId(String customerSpace, String modelGuid) {
        String url = constructUrl(URL_PREFX + "/modelid/{modelGuid}/event-table", shortenCustomerSpace(customerSpace),
                modelGuid);
        return get("get model event table", url, Table.class);
    }

    @Override
    public List<String> getRequiredColumnDisplayNames(String customerSpace, String modelGuid) {
        String url = constructUrl(URL_PREFX + "/modelid/{modelGuid}/required-column-names",
                shortenCustomerSpace(customerSpace), modelGuid);
        return getList("get model model required column names", url, String.class);
    }

    @Override
    public List<Attribute> getRequiredColumns(String customerSpace, String modelGuid) {
        String url = constructUrl(URL_PREFX + "/modelid/{modelGuid}/required-columns",
                shortenCustomerSpace(customerSpace), modelGuid);
        return getList("get model required columns", url, Attribute.class);
    }

    @Override
    public Set<String> getLatticeAttributeNames(String customerSpace, String modelGuid) {
        String url = constructUrl(URL_PREFX + "/modelid/{modelGuid}/lattice-attr-names",
                shortenCustomerSpace(customerSpace), modelGuid);
        return new HashSet<>(getList("get model lattice attrs", url, String.class));
    }

    @Override
    public List<Attribute> getAttributesFromFields(String customerSpace, List<Attribute> attributes,
            List<VdbMetadataField> fields) {
        String url = constructUrl(URL_PREFX + "/attributes-from-fields", shortenCustomerSpace(customerSpace));
        ModelFieldsToAttributesRequest request = new ModelFieldsToAttributesRequest();
        request.setAttributes(attributes);
        request.setFields(fields);
        List list = post("convert fields to attributes", url, request, List.class);
        return JsonUtils.convertList(list, Attribute.class);
    }

}
