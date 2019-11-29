package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.CreateCatalogRequest;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("activityStoreProxy")
public class ActivityStoreProxy extends MicroserviceRestApiProxy implements ProxyInterface {
    private static final String ROOT_PATH = "cdl";

    protected ActivityStoreProxy() {
        super(ROOT_PATH);
    }

    public ActivityStoreProxy(String hostPort) {
        super(hostPort, ROOT_PATH);
    }

    public Catalog createCatalog(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId,
            String primaryKeyColumn) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/catalogs",
                shortenCustomerSpace(customerSpace));
        CreateCatalogRequest request = new CreateCatalogRequest(catalogName, taskUniqueId, primaryKeyColumn);
        return post("create_catalog", url, request, Catalog.class);
    }

    public Catalog findCatalogByName(@NotNull String customerSpace, @NotNull String catalogName) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/catalogs/{name}",
                shortenCustomerSpace(customerSpace), catalogName);
        return get("find_catalog_by_name", url, Catalog.class);
    }

    public AtlasStream createStream(@NotNull String customerSpace, @NotNull AtlasStream stream) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/streams",
                shortenCustomerSpace(customerSpace));
        return post("create_stream", url, stream, AtlasStream.class);
    }

    public AtlasStream findStreamByName(@NotNull String customerSpace, @NotNull String streamName,
            boolean inflateDimensions) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/activities/streams/{name}?inflateDimensions={inflateDimensions}",
                shortenCustomerSpace(customerSpace), streamName, inflateDimensions);
        return get("find_stream_by_name", url, AtlasStream.class);
    }

    public StreamDimension updateDimension(@NotNull String customerSpace, @NotNull String streamName,
            @NotNull StreamDimension dimension) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/activities/streams/{streamName}/dimensions/{dimensionName}",
                shortenCustomerSpace(customerSpace), streamName, dimension.getName());
        return put("update_stream_dimension", url, dimension, StreamDimension.class);
    }

    public String saveDimensionMetadata(@NotNull String customerSpace,
            @NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/dimensionMetadata",
                shortenCustomerSpace(customerSpace));
        return post("save_dimension_metadata", url, dimensionMetadataMap, String.class);
    }

    public String saveDimensionMetadataWithSignature(@NotNull String customerSpace, @NotNull String signature,
            @NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/dimensionMetadata/{signature}",
                shortenCustomerSpace(customerSpace), signature);
        return post("save_dimension_metadata_with_signature", url, dimensionMetadataMap, String.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<String, DimensionMetadata>> getDimensionMetadata(@NotNull String customerSpace,
            String signature) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/dimensionMetadata",
                shortenCustomerSpace(customerSpace));
        if (StringUtils.isNotBlank(signature)) {
            url += "?signature=" + signature;
        }
        return get("get_dimension_metadata", url, Map.class);
    }

    public Map<String, DimensionMetadata> getDimensionMetadataInStream(@NotNull String customerSpace,
            @NotNull String streamName, String signature) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/dimensionMetadata/streams/{streamName}",
                shortenCustomerSpace(customerSpace), streamName);
        if (StringUtils.isNotBlank(signature)) {
            url += "?signature=" + signature;
        }
        Map<?, ?> rawMap = get("get_dimension_metadata_in_stream", url, Map.class);
        return JsonUtils.convertMap(rawMap, String.class, DimensionMetadata.class);
    }

    public void deleteDimensionMetadataWithSignature(@NotNull String customerSpace, @NotNull String signature) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/dimensionMetadata/{signature}",
                shortenCustomerSpace(customerSpace), signature);
        delete("delete_dimension_metadata_with_signature", url);
    }

    public ActivityMetricsGroup findGroupByGroupId(@NotNull String customerSpace, @NotNull String groupId) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/metricsGroups/groupId/{groupId}",
                shortenCustomerSpace(customerSpace), groupId);
        return get("findGroupByGroupId", url, ActivityMetricsGroup.class);
    }
}
