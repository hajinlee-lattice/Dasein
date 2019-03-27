package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.callable.QueryToTableIngestCallable;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("ingestInactiveRatings")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class IngestInactiveRatings extends BaseWorkflowStep<GenerateRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(IngestInactiveRatings.class);
    private static final int MAX_RESTRICTIONS = 256;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EntityProxy entityProxy;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        DataCollection.Version version = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        log.info("Read inactive version from workflow context: " + version);

        List<String> attrs = getListObjectFromContext(INACTIVE_ENGINE_ATTRIBUTES, String.class);

        if (CollectionUtils.isEmpty(attrs)) {
            throw new IllegalArgumentException("There is no inactive rating attributes to be queried");
        } else {
            log.info("Going to query " + CollectionUtils.size(attrs) + " attributes from redshift: " + attrs);
            List<Pair<String, Class<?>>> queryAttrs = new ArrayList<>();
            attrs.forEach(attr -> {
                Class<?> clz = String.class;
                for (RatingEngine.ScoreType scoreType: RatingEngine.SCORE_ATTR_SUFFIX.keySet()) {
                    if (attr.endsWith(RatingEngine.SCORE_ATTR_SUFFIX.get(scoreType))) {
                        clz = RatingEngine.SCORE_ATTR_CLZ.get(scoreType);
                    }
                }
                queryAttrs.add(Pair.of(attr, clz));
            });

            Restriction restriction = getRestriction(queryAttrs);
            queryAttrs.add(Pair.of(InterfaceName.AccountId.name(), String.class));
            queryAttrs.add(Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class));
            queryAttrs.add(Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));
            String targetTableName = NamingUtils.timestamp("InactiveRatings");
            FrontEndQuery query = getQuery(queryAttrs, restriction);

            QueryToTableIngestCallable callable = QueryToTableIngestCallable.builder() //
                    .yarnConfiguration(yarnConfiguration) //
                    .customerSpace(customerSpace.toString()) //
                    .entityProxy(entityProxy) //
                    .metadataProxy(metadataProxy) //
                    .query(query) //
                    .schema(queryAttrs) //
                    .version(version) //
                    .tableName(targetTableName) //
                    .build();
            callable.setPageSize(100_000);
            callable.setRowsPerFile(1_000_000);
            String generatedTable = callable.call();

            if (StringUtils.isNotBlank(generatedTable)) {
                putStringValueInContext(INACTIVE_RATINGS_TABLE_NAME, generatedTable);
            }
        }
    }

    private Restriction getRestriction(List<Pair<String, Class<?>>> queryAttrs) {
        List<Restriction> restrictions = new ArrayList<>();
        queryAttrs.forEach(pair -> {
            if (restrictions.size() <= MAX_RESTRICTIONS) {
                Restriction restriction = Restriction.builder() //
                        .let(BusinessEntity.Rating, pair.getLeft()).isNotNull().build();
                restrictions.add(restriction);
            }
        });
        List<Restriction> restrictions2;
        if (restrictions.size() > MAX_RESTRICTIONS) {
            restrictions2 = new ArrayList<>();
        } else {
            restrictions2 = restrictions;
        }
        restrictions2.add(Restriction.builder() //
                .let(BusinessEntity.Rating, InterfaceName.CDLCreatedTime.name()).isNotNull().build());
        return Restriction.builder().or(restrictions2).build();
    }

    private FrontEndQuery getQuery(List<Pair<String, Class<?>>> queryAttrs, Restriction restriction) {
        FrontEndQuery query = new FrontEndQuery();
        query.setMainEntity(BusinessEntity.Account);
        List<Lookup> attributeLookups = new ArrayList<>();
        queryAttrs.forEach(pair -> {
            AttributeLookup attributeLookup = new AttributeLookup();
            String attrName = pair.getLeft();
            attributeLookup.setAttribute(attrName);
            if (InterfaceName.AccountId.name().equals(attrName)) {
                attributeLookup.setEntity(BusinessEntity.Account);
            } else {
                attributeLookup.setEntity(BusinessEntity.Rating);
            }
            attributeLookups.add(attributeLookup);
        });
        query.setLookups(attributeLookups);
        AttributeLookup accountIdLookup = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
        query.setSort(new FrontEndSort(Collections.singletonList(accountIdLookup),false));
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        query.setAccountRestriction(frontEndRestriction);
        return query;
    }

}
