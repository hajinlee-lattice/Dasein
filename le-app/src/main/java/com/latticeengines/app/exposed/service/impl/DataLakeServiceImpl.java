package com.latticeengines.app.exposed.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.exposed.util.ImportanceOrderingUtils;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import reactor.core.publisher.Flux;

@Component("dataLakeService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DataLakeServiceImpl implements DataLakeService {

    private static final Logger log = LoggerFactory.getLogger(DataLakeServiceImpl.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private CacheManager cacheManager;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private final DataLakeService _dataLakeService;

    private LocalCacheManager<String, Map<String, StatsCube>> statsCubesCache;
    private LocalCacheManager<String, List<ColumnMetadata>> cmCache;

    @Inject
    public DataLakeServiceImpl(DataLakeService dataLakeService) {
        _dataLakeService = dataLakeService;
        statsCubesCache = new LocalCacheManager<>(CacheName.DataLakeStatsCubesCache, str -> {
            String[] tokens = str.split("\\|");
            String customerSpace = tokens[0];
            return getStatsCubes(customerSpace);
        }, 100); //
        cmCache = new LocalCacheManager<>(CacheName.DataLakeCMCache, str -> {
            String[] tokens = str.split("\\|");
            BusinessEntity entity = BusinessEntity.valueOf(tokens[1]);
            String customerSpace = tokens[0];
            return getServingMetadataForEntity(customerSpace, entity).collectList().block();
        }, 100); //
    }

    @PostConstruct
    private void postConstruct() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Arrays.asList(statsCubesCache, cmCache));
        }
    }

    @Override
    public long getAttributesCount() {
        return getAllSegmentAttributes().count().blockOptional().orElse(0L);
    }

    @Override
    public List<ColumnMetadata> getAttributes(Integer offset, Integer max) {
        Flux<ColumnMetadata> stream = getAllSegmentAttributes();
        try {
            if (offset != null) {
                stream = stream.skip(offset);
            }
            if (max != null) {
                stream = stream.take(max);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18143);
        }
        return stream.collectList().block();
    }

    private Flux<ColumnMetadata> getAllSegmentAttributes() {
        String tenantId = MultiTenantContext.getTenantId();
        return Flux.fromIterable(BusinessEntity.SEGMENT_ENTITIES) //
                .flatMap(entity -> Flux.fromIterable(_dataLakeService.getCachedServingMetadataForEntity(tenantId, entity))) //
                .filter(cm -> cm.isEnabledFor(ColumnSelection.Predefined.Segment)) //
                .filter(cm -> !StatsCubeUtils.shouldHideAttr(cm));
    }

    @Override
    public List<ColumnMetadata> getAttributesInPredefinedGroup(ColumnSelection.Predefined predefined) {
        // Only return attributes for account now
        String tenantId = MultiTenantContext.getTenantId();
        List<ColumnMetadata> cms = _dataLakeService.getCachedServingMetadataForEntity(tenantId, BusinessEntity.Account);
        return cms.stream().filter(cm -> cm.getGroups() != null && cm.isEnabledFor(predefined)
                // Hack to limit attributes for talking points temporarily PLS-7065
                && (cm.getCategory().equals(Category.ACCOUNT_ATTRIBUTES)
                || cm.getCategory().equals(Category.FIRMOGRAPHICS))) //
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, StatsCube> getStatsCubes() {
        String tenantId = MultiTenantContext.getTenantId();
        return _dataLakeService.getStatsCubes(tenantId);
    }

    @Override
    public TopNTree getTopNTree() {
        String tenantId = MultiTenantContext.getTenantId();
        return _dataLakeService.getTopNTree(tenantId);
    }

    @Override
    public AttributeStats getAttributeStats(BusinessEntity entity, String attribute) {
        Map<String, StatsCube> cubes = getStatsCubes();
        if (MapUtils.isEmpty(cubes) || !cubes.containsKey(entity.name())) {
            return null;
        }
        StatsCube cube = cubes.get(entity.name());
        if (cube.getStatistics().containsKey(attribute)) {
            return cube.getStatistics().get(attribute);
        } else {
            log.warn("Did not find attribute stats for " + entity + "." + attribute);
            return null;
        }
    }

    @Override
    public DataPage getAccountById(String accountID, ColumnSelection.Predefined predefined) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

        List<String> attributes = getAttributesInPredefinedGroup(predefined).stream() //
                .map(ColumnMetadata::getAttrName).collect(Collectors.toList());

        attributes.add(InterfaceName.AccountId.name());
        attributes.add(InterfaceName.SalesforceAccountID.name());

        if (!StringUtils.isNotEmpty(accountID)) {
            throw new LedpException(LedpCode.LEDP_39001, new String[] { accountID, customerSpace });
        }

        Restriction accRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                .eq(accountID) //
                .build();

        Restriction sfdcRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.SalesforceAccountID.name()) //
                .eq(accountID) //
                .build();
        Restriction restriction = Restriction.builder().or(Arrays.asList(accRestriction, sfdcRestriction)).build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(restriction));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.Account, attributes.toArray(new String[attributes.size()]));

        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        return entityProxy.getData(customerSpace, frontEndQuery);
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|topn\", #customerSpace)")
    public TopNTree getTopNTree(String customerSpace) {
        Map<String, StatsCube> cubes = getStatsCubes();
        if (MapUtils.isEmpty(cubes)) {
            return null;
        }
        Map<String, List<ColumnMetadata>> cmMap = getColumnMetadataMap(customerSpace, cubes);
        if (MapUtils.isEmpty(cmMap)) {
            return null;
        }
        String timerMsg = "Construct top N tree with " + cubes.size() + " cubes.";
        try (PerformanceTimer timer = new PerformanceTimer(timerMsg)) {
            return StatsCubeUtils.constructTopNTree(cubes, cmMap, true);
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|statscubes\", #customerSpace)", sync = true)
    public Map<String, StatsCube> getStatsCubes(String customerSpace) {
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            Map<String, StatsCube> cubes = container.getStatsCubes();
            Map<String, List<ColumnMetadata>> cmMap = getColumnMetadataMap(customerSpace, cubes);
            return StatsCubeUtils.filterStatsCube(cubes, cmMap);
        }
        return null;
    }

    private Map<String, List<ColumnMetadata>> getColumnMetadataMap(String customerSpace, Map<String, StatsCube> cubes) {
        Map<String, List<ColumnMetadata>> cmMap = new HashMap<>();
        cubes.keySet().forEach(key -> {
            BusinessEntity entity = BusinessEntity.valueOf(key);
            String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
            List<ColumnMetadata> cms = _dataLakeService.getCachedServingMetadataForEntity(tenantId, entity);
            if (CollectionUtils.isNotEmpty(cms)) {
                cmMap.put(key, cms);
            }
        });
        return cmMap;
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeCMCacheName, key = "T(java.lang.String).format(\"%s|%s|metadata\", #customerSpace, #entity)", sync = true)
    public List<ColumnMetadata> getCachedServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        return getServingMetadataForEntity(customerSpace, entity).collectList().block();
    }

    private Flux<ColumnMetadata> getServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (entity != null) {
            cms = servingStoreProxy.getDecoratedMetadata(customerSpace, entity);
            if (cms != null) {
                if (BusinessEntity.Account.equals(entity)) {
                    cms = ImportanceOrderingUtils.addImportanceOrdering(cms);
                }
                AtomicLong counter = new AtomicLong();
                AtomicLong timer = new AtomicLong();
                cms = cms.doOnSubscribe(s -> timer.set(System.currentTimeMillis())) //
                        .doOnNext(cm -> counter.getAndIncrement()).doOnComplete(() -> {
                            long duration = System.currentTimeMillis() - timer.get();
                            long count = counter.get();
                            String msg = "Retrieved " + count + " attributes from serving store proxy for " + entity
                                    + " in " + customerSpace + " TimeElapsed=" + duration + " msec.";
                            log.info(msg);
                        });
            }
        }
        return cms;
    }
}
