package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.EXPORT_SCHEMA_MAP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;


@Component("extractAtlasEntity")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractAtlasEntity extends BaseSparkSQLStep<EntityExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExtractAtlasEntity.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;
    private Map<BusinessEntity, List<ColumnMetadata>> schemaMap;

    @Override
    public void execute() {
        customerSpace = parseCustomerSpace(configuration);
        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);
        schemaMap = getExportSchema();
        WorkflowStaticContext.putObject(EXPORT_SCHEMA_MAP, schemaMap);

        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        Map<ExportEntity, HdfsDataUnit> resultMap = retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
            }
            Map<ExportEntity, HdfsDataUnit> resultForCurrentAttempt = new HashMap<>();
            try {
                startLivySession(getHdfsPaths(attrRepo));
                configuration.getExportEntities().forEach(exportEntity -> {
                    FrontEndQuery frontEndQuery = getFrontEndQueryCopy();
                    HdfsDataUnit entityResult = exportOneEntity(exportEntity, frontEndQuery);
                    resultForCurrentAttempt.put(exportEntity, entityResult);
                });
            } finally {
                stopLivySession();
            }
            return resultForCurrentAttempt;
        });

        putObjectInContext(ATLAS_EXPORT_DATA_UNIT, resultMap);
    }

    @Override
    protected CustomerSpace parseCustomerSpace(EntityExportStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(EntityExportStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getDataCollectionVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(EntityExportStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(EntityExportStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            throw new RuntimeException("Cannot find attribute repo in context");
        }
        return attrRepo;
    }

    private FrontEndQuery getFrontEndQueryCopy() {
        FrontEndQuery inConfig = configuration.getFrontEndQuery();
        FrontEndQuery frontEndQuery;
        if (inConfig != null) {
            frontEndQuery = JsonUtils.deserialize(JsonUtils.serialize(inConfig), FrontEndQuery.class);
            inConfig.setPageFilter(null);
        } else {
            frontEndQuery = new FrontEndQuery();
        }
        return frontEndQuery;
    }

    private HdfsDataUnit exportOneEntity(ExportEntity exportEntity, FrontEndQuery frontEndQuery) {
        List<Lookup> lookups;
        if (ExportEntity.Account.equals(exportEntity)) {
            frontEndQuery.setMainEntity(BusinessEntity.Account);
            lookups = new ArrayList<>();
            for (BusinessEntity entity: BusinessEntity.EXPORT_ENTITIES) {
                if (!BusinessEntity.Contact.equals(entity)) {
                    List<ColumnMetadata> cms = schemaMap.getOrDefault(entity, Collections.emptyList());
                    cms.forEach(cm -> lookups.add(new AttributeLookup(entity, cm.getAttrName())));
                }
            }
        } else if (ExportEntity.Contact.equals(exportEntity)) {
            frontEndQuery.setMainEntity(BusinessEntity.Contact);
            List<ColumnMetadata> cms = schemaMap.getOrDefault(BusinessEntity.Contact, Collections.emptyList());
            lookups = cms.stream() //
                    .map(cm -> new AttributeLookup(BusinessEntity.Contact, cm.getAttrName())) //
                    .collect(Collectors.toList());
        } else {
            throw new UnsupportedOperationException("Unknown export entity " + exportEntity);
        }
        log.info("Going to export " + lookups.size() + " columns for " + exportEntity);
        frontEndQuery.setLookups(lookups);
        return getEntityQueryData(frontEndQuery);
    }

    private Map<BusinessEntity, List<ColumnMetadata>> getExportSchema() {
        List<ColumnSelection.Predefined> groups = Collections.singletonList(ColumnSelection.Predefined.Enrichment);
        Map<BusinessEntity, List<ColumnMetadata>> schemaMap = new HashMap<>();
        for (BusinessEntity entity: BusinessEntity.EXPORT_ENTITIES) {
            List<ColumnMetadata> cms = servingStoreProxy //
                    .getDecoratedMetadata(customerSpace.toString(), entity, groups, version).collectList().block();
            if (CollectionUtils.isNotEmpty(cms)) {
                schemaMap.put(entity, cms);
            }
            log.info("Found " + CollectionUtils.size(cms) + " attrs to export for " + entity);
            cms.forEach(cm -> {
                log.info("Attribute Name: " + cm.getAttrName());
            });
        }
        return schemaMap;
    }

}
