package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.ListSegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.MetadataSegmentExportEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.impl.CDLDependencyChecker;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.cdl.CreateDataTemplateRequest;
import com.latticeengines.domain.exposed.cdl.UpdateSegmentCountResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.util.SegmentDependencyUtil;
import com.latticeengines.domain.exposed.util.SegmentUtils;
import com.latticeengines.metadata.service.DataTemplateService;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {

    private static final Logger log = LoggerFactory.getLogger(SegmentServiceImpl.class);

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private ListSegmentEntityMgr listSegmentEntityMgr;

    @Inject
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private CDLDependencyChecker dependencyChecker;

    @Inject
    private MetadataSegmentExportEntityMgr metadataSegmentExportEntityMgr;

    @Inject
    private DataTemplateService dataTemplateService;

    @Value("${aws.s3.data.stage.bucket}")
    private String dateStageBucket;

    private final String listSegmentCSVAdaptorPath = "metadata/ListSegmentCSVAdaptor.json";

    @Override
    public MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        validateSegment(segment);
        MetadataSegment persistedSegment;
        if (StringUtils.isNotBlank(segment.getName())) {
            MetadataSegment existingSegment = findByName(segment.getName());
            if (existingSegment != null) {
                persistedSegment = segmentEntityMgr.updateSegment(segment, existingSegment);
                evictRatingMetadataCache(existingSegment, segment);
            } else {
                persistedSegment = segmentEntityMgr.createSegment(segment);
            }
        } else {
            segment.setName(NamingUtils.timestampWithRandom("Segment"));
            persistedSegment = segmentEntityMgr.createSegment(segment);
        }
        if (persistedSegment != null && !MetadataSegment.SegmentType.List.equals(persistedSegment.getType())) {
            try {
                Map<BusinessEntity, Long> counts = updateSegmentCounts(persistedSegment);
                persistedSegment.setAccounts(counts.getOrDefault(BusinessEntity.Account, 0L));
                persistedSegment.setContacts(counts.getOrDefault(BusinessEntity.Contact, 0L));
            } catch (Exception e) {
                log.warn("Failed to update segment counts.", e);
            }
        }
        return persistedSegment;
    }

    @Override
    public MetadataSegment createOrUpdateListSegment(MetadataSegment segment) {
        MetadataSegment persistedSegment = null;
        if (segment.getListSegment() != null) {
            MetadataSegment existingSegment = segmentEntityMgr.findByExternalInfo(segment);
            if (existingSegment != null) {
                persistedSegment = segmentEntityMgr.updateListSegment(segment, existingSegment);
            } else {
                if (StringUtils.isEmpty(existingSegment.getName())){
                    segment.setName(NamingUtils.timestampWithRandom("Segment"));
                }
                persistedSegment = createListSegment(segment);
            }
        } else if (StringUtils.isNotEmpty(segment.getName())) {
            MetadataSegment existingSegment = segmentEntityMgr.findByName(segment.getName());
            if (existingSegment != null) {
                persistedSegment = segmentEntityMgr.updateListSegment(segment, existingSegment);
            } else {
                persistedSegment = createListSegment(segment);
            }
        } else {
            log.error("Can't create or update list segment with empty name or empty list segment object.");
        }
        return persistedSegment;
    }

    private CSVAdaptor readCSVAdaptor() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(listSegmentCSVAdaptorPath)) {
            return JsonUtils.deserialize(inputStream, CSVAdaptor.class);
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read " + listSegmentCSVAdaptorPath, exception);
        }
    }

    private MetadataSegment createListSegment(MetadataSegment segment) {
        if (segment.getListSegment() != null) {
            ListSegment listSegment = segment.getListSegment();
            String s3Path = S3PathBuilder.getS3ListSegmentDir(dateStageBucket, MultiTenantContext.getShortTenantId(), segment.getName());
            s3Path = s3Path //
                    .replaceFirst("s3a:", "s3:").replaceFirst("s3n:", "s3:");
            listSegment.setS3DropFolder(s3Path);
            listSegment.setCsvAdaptor(readCSVAdaptor());
        }
        return segmentEntityMgr.createListSegment(segment);
    }

    @Override
    public ListSegment updateListSegment(ListSegment segment) {
        return listSegmentEntityMgr.updateListSegment(segment);
    }

    @Override
    public Boolean deleteSegmentByName(String segmentName, boolean ignoreDependencyCheck, boolean hardDelete) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName, hardDelete);
        if (segment == null) {
            return false;
        }
        segmentEntityMgr.delete(segment, ignoreDependencyCheck, hardDelete);
        return true;
    }

    @Override
    public boolean deleteSegmentByExternalInfo(String externalSystem, String externalSegmentId, boolean hardDelete) {
        MetadataSegment segment = segmentEntityMgr.findByExternalInfo(externalSystem, externalSegmentId);
        if (segment == null) {
            return false;
        }
        segmentEntityMgr.delete(segment, false, hardDelete);
        return true;
    }

    @Override
    public Boolean revertDeleteSegmentByName(String segmentName) {
        segmentEntityMgr.revertDelete(segmentName);
        return true;
    }

    @Override
    public List<String> getAllDeletedSegments() {
        return segmentEntityMgr.getAllDeletedSegments();
    }

    @Override
    public List<MetadataSegment> getSegments() {
        String collectionName = dataCollectionEntityMgr.findDefaultCollection().getName();
        List<MetadataSegment> result = segmentEntityMgr.findAllInCollection(collectionName);
        result.stream().forEach(segment -> TeamUtils.fillTeamId(segment));
        return result;
    }

    @Override
    public List<MetadataSegment> getListSegments() {
        return segmentEntityMgr.findByType(MetadataSegment.SegmentType.List);
    }

    @Override
    public MetadataSegment findByName(String name) {
        MetadataSegment segment = segmentEntityMgr.findByName(name);
        if (segment != null && Boolean.TRUE.equals(segment.getCountsOutdated())) {
            log.info("Segment {}  has outdated count, trying to update it.", segment.getName());
            try {
                Map<BusinessEntity, Long> counts = updateSegmentCounts(segment);
                segment.setAccounts(counts.get(BusinessEntity.Account));
                segment.setContacts(counts.get(BusinessEntity.Contact));
            } catch (Exception e) {
                log.warn("Failed to update segment counts.", e);
            }
        }
        TeamUtils.fillTeamId(segment);
        return segment;
    }

    @Override
    public MetadataSegment findListSegmentByName(String name) {
        return segmentEntityMgr.findByName(name, true);
    }

    @Override
    public MetadataSegment findMaster(String collectionName) {
        return segmentEntityMgr.findMasterSegment(collectionName);
    }

    @Override
    public StatisticsContainer getStats(String segmentName, DataCollection.Version version) {
        if (version == null) {
            // by default get from active version
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        return statisticsContainerEntityMgr.findInSegment(segmentName, version);
    }

    @Override
    public void upsertStats(String segmentName, StatisticsContainer statisticsContainer) {
        segmentEntityMgr.upsertStats(segmentName, statisticsContainer);
    }

    @Override
    public Map<BusinessEntity, Long> updateSegmentCounts(String segmentName) {
        Map<BusinessEntity, Long> map = new HashMap<>();
        MetadataSegment existingSegment = findByName(segmentName);
        if (existingSegment != null) {
            map = updateSegmentCounts(existingSegment);
        }
        return map;
    }

    @Override
    public void updateSegmentsCountsAsync() {
        final Tenant tenant = MultiTenantContext.getTenant();
        new Thread(() -> {
            MultiTenantContext.setTenant(tenant);
            List<MetadataSegment> segments = getSegments();
            if (CollectionUtils.isNotEmpty(segments)) {
                segments.forEach(segment -> {
                    if (!SegmentUtils.hasListSegment(segment)) {
                        MetadataSegment segmentCopy = segment.getDeepCopy();
                        segmentCopy.setCountsOutdated(true);
                        segmentEntityMgr.updateSegmentWithoutActionAndAuditing(segmentCopy, segment);
                    }
                });
            }
            UpdateSegmentCountResponse response = updateSegmentsCounts();
            log.info("UpdateSegmentCountResponse={}", JsonUtils.serialize(response));
        }).start();
    }

    @Override
    public UpdateSegmentCountResponse updateSegmentsCounts() {
        log.info("Start updating counts for all segment for " + MultiTenantContext.getShortTenantId());
        List<String> failedSegments = new ArrayList<>();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<MetadataSegment> segments = getSegments();
            log.info("Updating counts for " + CollectionUtils.size(segments) + " segments.");
            Map<String, Map<BusinessEntity, Long>> review = new HashMap<>();
            if (CollectionUtils.isNotEmpty(segments)) {
                // update the most recently touched segments first
                segments.sort((o1, o2) -> {
                    Date updated1 = o1.getUpdated();
                    Date updated2 = o2.getUpdated();
                    if (updated1 == null) {
                        updated1 = o1.getCreated();
                    }
                    if (updated2 == null) {
                        updated2 = o2.getCreated();
                    }
                    if (updated1 != null && updated2 != null) {
                        return updated2.compareTo(updated1);
                    } else if (updated1 != null) {
                        return -1;
                    } else if (updated2 != null) {
                        return 1;
                    } else {
                        return 0;
                    }
                });
                // Do not parallel, as it will be bottle-necked at objectapi
                segments.forEach(segment -> {
                    String name = segment.getName();
                    try {
                        Map<BusinessEntity, Long> counts = updateSegmentCounts(segment);
                        review.put(name, counts);
                    } catch (Exception e) {
                        log.warn("Failed to update counts for segment " + name + //
                                " in tenant " + MultiTenantContext.getShortTenantId());
                        failedSegments.add(name);
                    }
                });
            }
            timer.setTimerMessage("Finished updating counts for " + CollectionUtils.size(segments) + " segments.");
            UpdateSegmentCountResponse response = new UpdateSegmentCountResponse();
            response.setUpdatedCounts(review);
            response.setFailedSegments(failedSegments);
            return response;
        }
    }

    private Map<BusinessEntity, Long> getEntityCounts(MetadataSegment segment) {
        Map<BusinessEntity, Long> map = new HashMap<>();
        FrontEndRestriction accountRestriction = segment.getAccountFrontEndRestriction();
        if (accountRestriction == null) {
            accountRestriction = new FrontEndRestriction(segment.getAccountRestriction());
        }
        FrontEndRestriction contactRestriction = segment.getContactFrontEndRestriction();
        if (contactRestriction == null) {
            contactRestriction = new FrontEndRestriction(segment.getContactRestriction());
        }
        for (BusinessEntity entity : BusinessEntity.COUNT_ENTITIES) {
            try {
                Long count = getEntityCount(entity, accountRestriction, contactRestriction);
                if (count != null) {
                    map.put(entity, count);
                }
            } catch (Exception e) {
                log.warn("Failed to count " + entity + ": " + e.getMessage());
            }
        }
        return map;
    }

    private Map<BusinessEntity, Long> updateSegmentCounts(MetadataSegment segment) {
        if (!MetadataSegment.SegmentType.List.equals(segment.getType())) {
            // use a deep copy to avoid changing restriction format to break UI
            MetadataSegment segmentCopy = segment.getDeepCopy();
            Map<BusinessEntity, Long> counts = getEntityCounts(segmentCopy);
            counts.forEach(segmentCopy::setEntityCount);
            log.info("Updating counts for segment " + segment.getName() + " (" + segment.getDisplayName() + ")" //
                    + " to " + JsonUtils.serialize(segmentCopy.getEntityCounts()));
            segmentCopy.setCountsOutdated(false);
            segment = segmentEntityMgr.updateSegmentWithoutActionAndAuditing(segmentCopy, segment);
        }
        return segment.getEntityCounts();
    }

    private Long getEntityCount(BusinessEntity entity, FrontEndRestriction accountRestriction,
                                FrontEndRestriction contactRestriction) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        Restriction accountExists = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()).isNotNull().build();
        if (accountRestriction != null && accountRestriction.getRestriction() != null) {
            if (BusinessEntity.Contact.equals(entity)) {
                Restriction combined = Restriction.builder() //
                        .and(accountRestriction.getRestriction(), accountExists).build();
                frontEndQuery.setAccountRestriction(new FrontEndRestriction(combined));
            } else {
                frontEndQuery.setAccountRestriction(accountRestriction);
            }
        } else if (BusinessEntity.Contact.equals(entity)) {
            frontEndQuery.setAccountRestriction(new FrontEndRestriction(accountExists));
        }
        if (contactRestriction != null && contactRestriction.getRestriction() != null) {
            frontEndQuery.setContactRestriction(contactRestriction);
        }
        frontEndQuery.setMainEntity(entity);
        return entityProxy.getCount(customerSpace, frontEndQuery);
    }

    @Override
    public Map<String, List<String>> getDependencies(String segmentName) throws Exception {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return dependencyChecker.getDependencies(customerSpace, segmentName, CDLObjectTypes.Segment.name());
    }

    // Only when a segment displayName got changed, then we need to evict this
    // DataLakeCache
    private void evictRatingMetadataCache(MetadataSegment existingSegment, MetadataSegment updatedSegment) {
        if (existingSegment == null || updatedSegment == null) {
            return;
        }
        if (StringUtils.equalsAny(existingSegment.getDisplayName(), updatedSegment.getDisplayName())) {
            return;
        }
        String tenantId = MultiTenantContext.getShortTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.getCdlServingCacheGroup());
    }

    @Override
    public List<AttributeLookup> findDependingAttributes(List<MetadataSegment> metadataSegments) {
        Set<AttributeLookup> dependingAttributes = new HashSet<>();
        if (metadataSegments != null) {
            for (MetadataSegment metadataSegment : metadataSegments) {
                SegmentDependencyUtil.findSegmentDependingAttributes(metadataSegment);
                Set<AttributeLookup> attributeLookups = metadataSegment.getSegmentAttributes();
                if (attributeLookups != null) {
                    dependingAttributes.addAll(metadataSegment.getSegmentAttributes());
                }
            }
        }

        return new ArrayList<>(dependingAttributes);
    }

    @Override
    public List<MetadataSegment> findDependingSegments(List<String> attributes) {
        List<MetadataSegment> dependingMetadataSegments = new ArrayList<>();
        if (attributes != null) {
            List<MetadataSegment> metadataSegments = getSegments();
            if (metadataSegments != null) {
                for (MetadataSegment metadataSegment : metadataSegments) {
                    SegmentDependencyUtil.findSegmentDependingAttributes(metadataSegment);
                    Set<AttributeLookup> segmentAttributes = metadataSegment.getSegmentAttributes();
                    if (segmentAttributes != null) {
                        for (AttributeLookup attributeLookup : segmentAttributes) {
                            if (attributes.contains(sanitize(attributeLookup.toString()))) {
                                dependingMetadataSegments.add(metadataSegment);
                                break;
                            }
                        }
                    }
                }
            }
        }

        return dependingMetadataSegments;
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }

    private void validateSegment(MetadataSegment segment) {
        List<BucketRestriction> invalidBkts = new ArrayList<>();
        try {
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(segment.getAccountRestriction()));
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(segment.getContactRestriction()));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40057, e, new String[]{e.getMessage()});
        }
        if (CollectionUtils.isNotEmpty(invalidBkts)) {
            String message = invalidBkts.stream() //
                    .map(BucketRestriction::getAttr) //
                    .map(AttributeLookup::toString) //
                    .collect(Collectors.joining(","));
            throw new LedpException(LedpCode.LEDP_40057, new String[]{message});
        }
    }

    @Override
    public MetadataSegmentExport getMetadataSegmentExport(String exportId) {
        return metadataSegmentExportEntityMgr.findByExportId(exportId);
    }

    @Override
    public MetadataSegmentExport updateMetadataSegmentExport(String exportId, MetadataSegmentExport.Status state) {
        MetadataSegmentExport metadataSegmentExport = metadataSegmentExportEntityMgr.findByExportId(exportId);
        if (metadataSegmentExport != null) {
            metadataSegmentExport.setStatus(state);
            metadataSegmentExportEntityMgr.createOrUpdate(metadataSegmentExport);
            return metadataSegmentExportEntityMgr.findByExportId(metadataSegmentExport.getExportId());
        } else {
            return null;
        }
    }

    @Override
    public void deleteMetadataSegmentExport(String exportId) {
        metadataSegmentExportEntityMgr.deleteByExportId(exportId);
    }

    @Override
    public List<MetadataSegmentExport> getMetadataSegmentExports() {
        return metadataSegmentExportEntityMgr.findAll();
    }

    @Override
    public MetadataSegment findByExternalInfo(String externalSystem, String externalSegmentId) {
        return segmentEntityMgr.findByExternalInfo(externalSystem, externalSegmentId);
    }

    @Override
    public String createOrUpdateDataTemplate(String segmentName, CreateDataTemplateRequest request) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName, true);
        if (SegmentUtils.hasListSegment(segment)) {
            String tenantId = MultiTenantContext.getShortTenantId();
            ListSegment listSegment = segment.getListSegment();
            String templateId = listSegment.getTemplateId(request.getTemplateKey());
            DataTemplate dataTemplate = request.getDataTemplate();
            if (StringUtils.isEmpty(templateId)) {
                dataTemplate.setTenant(tenantId);
                templateId = dataTemplateService.create(dataTemplate);
                Map<String, String> dataTemplates = listSegment.getDataTemplates();
                if (MapUtils.isEmpty(dataTemplates)) {
                    dataTemplates = new HashMap<>();
                }
                dataTemplates.put(request.getTemplateKey(), templateId);
                listSegment.setDataTemplates(dataTemplates);
                listSegmentEntityMgr.updateListSegment(listSegment);
            } else {
                dataTemplateService.updateByUuid(templateId, dataTemplate);
            }
            return templateId;
        } else {
            throw new LedpException(LedpCode.LEDP_00002, new RuntimeException("List segment does not exists"));
        }
    }

    private BusinessEntity getBusinessEntity(List<BusinessEntity> entities) {
        BusinessEntity entity;
        if (entities.contains(BusinessEntity.Account)) {
            entity = BusinessEntity.Account;
        } else if (entities.contains(BusinessEntity.Contact)) {
            entity = BusinessEntity.Contact;
        } else {
            throw new LedpException(LedpCode.LEDP_00002, new RuntimeException("List segment can only support account or contact entity."));
        }
        return entity;
    }

    @Override
    public Map<String, ColumnMetadata> getListSegmentMetadataMap(String segmentName, List<BusinessEntity> entities) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName, true);
        if (SegmentUtils.hasListSegment(segment)) {
            ListSegment listSegment = segment.getListSegment();
            BusinessEntity entity = getBusinessEntity(entities);
            String templateId = listSegment.getTemplateId(entity.name());
            if (StringUtils.isNotEmpty(templateId)) {
                return dataTemplateService.getTemplateMetadata(templateId, entity);
            } else {
                return Collections.emptyMap();
            }
        } else {
            log.info("can't find list segment info for segment {}.", segmentName);
            throw new LedpException(LedpCode.LEDP_00002, new RuntimeException("List segment does not exists"));
        }
    }
}
