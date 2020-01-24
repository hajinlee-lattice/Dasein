package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Component
public class ServingStoreProxyImpl extends MicroserviceRestApiProxy implements ServingStoreProxy {

    @Inject
    private ServingStoreCacheService cacheService;

    protected ServingStoreProxyImpl() {
        super("cdl");
    }

    @Override
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity) {
        return cacheService.getDecoratedMetadata(customerSpace, entity);
    }

    @Override
    public Set<String> getServingStoreColumnsFromCache(String customerSpace, BusinessEntity entity) {
        return cacheService.getServingTableColumns(customerSpace, entity);
    }

    @Override
    public List<ColumnMetadata> getAccountMetadataFromCache(String customerSpace, ColumnSelection.Predefined group) {
        return getDecoratedMetadataWithDeflatedDisplayNamesFromCache(customerSpace,
                BusinessEntity.getAccountExportEntities(group), Collections.singleton(group));
    }

    /**
     * deflateDisplayName means prepend sub-category in front of display name In "My
     * Data" page we can use sub-category to show hierarchical display name of
     * attributes But in other cases, such as TalkingPoint, we have to concatenate
     * sub-category and display name together
     */
    private List<ColumnMetadata> getDecoratedMetadataWithDeflatedDisplayNamesFromCache(String customerSpace,
            Collection<BusinessEntity> entities, Collection<ColumnSelection.Predefined> groups) {
        List<ColumnMetadata> allAttrs = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(entities)) {
            for (BusinessEntity entity : entities) {
                List<ColumnMetadata> entityAttrs = getDecoratedMetadataFromCache(customerSpace, entity);
                if (CollectionUtils.isNotEmpty(entityAttrs)) {
                    Stream<ColumnMetadata> stream = entityAttrs.stream();
                    if (groups != null) {
                        stream = stream.filter(cm -> cm.isEnabledForAny(groups));
                    }
                    if (BusinessEntity.ENTITIES_WITH_HIRERARCHICAL_DISPLAY_NAME.contains(entity)) {
                        stream = stream.peek(cm -> {
                            String subCategory = cm.getSubcategory();
                            if (StringUtils.isNotBlank(subCategory) && !"Others".equalsIgnoreCase(subCategory)) {
                                String displayName = cm.getDisplayName();
                                cm.setDisplayName(subCategory + ": " + displayName);
                            }
                        });
                    }
                    allAttrs.addAll(stream.collect(Collectors.toList()));
                }
            }
        }
        return allAttrs;
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        if (CollectionUtils.isNotEmpty(groups)) {
            url += "?groups=" + StringUtils.join(groups, ",");
        }
        List<ColumnMetadata> list = getList("serving store metadata", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups, DataCollection.Version version) {
        return getDecoratedMetadata(customerSpace, entity, groups, version, null);
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
             List<ColumnSelection.Predefined> groups, Version version, StoreFilter filter) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        url += getVersionGroupFilterParam(version, groups, filter);
        List<ColumnMetadata> list = getList("serving store metadata", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    private String getVersionGroupFilterParam(DataCollection.Version version,
                                              Collection<ColumnSelection.Predefined> groups, StoreFilter filter) {
        StringBuilder url = new StringBuilder();
        boolean firstParam = true;
        if (version != null) {
            url.append("?version=").append(version.toString());
            firstParam = false;
        }
        if (CollectionUtils.isNotEmpty(groups)) {
            if (firstParam) {
                url.append("?groups=").append(StringUtils.join(groups, ","));
                firstParam = false;
            } else {
                url.append("&groups=").append(StringUtils.join(groups, ","));
            }
        }
        if (filter != null) {
            if (firstParam) {
                url.append("?filter=").append(filter.toString());
            } else {
                url.append("&filter=").append(filter.toString());
            }
        }
        return url.toString();
    }

    @Override
    public List<ColumnMetadata> getAccountMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version) {
        return getDecoratedMetadataWithDeflatedDisplayName(customerSpace,
                BusinessEntity.getAccountExportEntities(group), Collections.singleton(group), version);
    }

    @Override
    public List<ColumnMetadata> getContactMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version) {
        return getDecoratedMetadataWithDeflatedDisplayName(customerSpace,
                Collections.singletonList(BusinessEntity.Contact), Collections.singleton(group), version);
    }

    private List<ColumnMetadata> getDecoratedMetadataWithDeflatedDisplayName(String customerSpace,
            Collection<BusinessEntity> entities, Collection<ColumnSelection.Predefined> groups,
            DataCollection.Version version) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        Map<BusinessEntity, List<ColumnMetadata>> map = entities.stream().parallel()
                .collect(Collectors.toMap(entity -> entity, entity -> {
                    MultiTenantContext.setTenant(tenant);
                    List<ColumnMetadata> cms = getDecoratedMetadata(customerSpace, entity, new ArrayList<>(groups),
                            version).collectList().block();
                    if (CollectionUtils.isNotEmpty(cms)
                            && BusinessEntity.ENTITIES_WITH_HIRERARCHICAL_DISPLAY_NAME.contains(entity)) {
                        cms.forEach(cm -> {
                            String subCategory = cm.getSubcategory();
                            if (StringUtils.isNotBlank(subCategory) && !"Others".equalsIgnoreCase(subCategory)) {
                                String displayName = cm.getDisplayName();
                                cm.setDisplayName(subCategory + ": " + displayName);
                            }
                        });
                    }
                    return CollectionUtils.isNotEmpty(cms) ? cms : new ArrayList<>();
                }));
        map.values().forEach(columnMetadataList::addAll);
        return columnMetadataList;
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace) {
        return getNewModelingAttrs(customerSpace, null);
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, Version version) {
        return getNewModelingAttrs(customerSpace, null, null);
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity, Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/new-modeling",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (version != null) {
            params.add("version=" + version.toString());
        }
        if (entity != null) {
            params.add("entity=" + entity);
        }
        if (CollectionUtils.isNotEmpty(params)) {
            url += "?" + StringUtils.join(params, "&");
        }
        List<ColumnMetadata> list = getList("serving store new modeling", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace) {
        return getAllowedModelingAttrs(customerSpace, false, null);
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, Boolean allCustomerAttrs,
            Version version) {
        return getAllowedModelingAttrs(customerSpace, null, allCustomerAttrs, version);
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, BusinessEntity entity,
            Boolean allCustomerAttrs, Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/allow-modeling",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (version != null) {
            params.add("version=" + version.toString());
        }
        if (Boolean.TRUE.equals(allCustomerAttrs)) {
            params.add("all-customer-attrs=1");
        }
        if (entity != null) {
            params.add("entity=" + entity);
        }
        if (CollectionUtils.isNotEmpty(params)) {
            url += "?" + StringUtils.join(params, "&");
        }
        List<ColumnMetadata> list = getList("serving store allowed modeling", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

}
