package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ActivityMetricsDecoratorFac;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;


@Component("activityMetricsDecorator")
public class ActivityMetricsDecoratorFacImpl implements ActivityMetricsDecoratorFac {

    // Entity match enabled or not doesn't impact activity metrics metadata
    private static final Set<String> systemAttributes = SchemaRepository //
            .getSystemAttributes(BusinessEntity.DepivotedPurchaseHistory, false).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();

        // TODO: wire service to handle deprecated attrs

        if (StringUtils.isNotBlank(tenantId)) {
            return new Decorator() {

                @Override
                public String getName() {
                    return "metric-attrs";
                }

                @Override
                public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
                    return metadata.map(ActivityMetricsDecoratorFacImpl::staticFilter);
                }

                @Override
                public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
                    return metadata.map(ActivityMetricsDecoratorFacImpl::staticFilter);
                }
            };
        } else {
            return new DummyDecorator();
        }

    }

    /**
     * Static logic applied to all tenants
     */
    private static ColumnMetadata staticFilter(ColumnMetadata cm) {
        String attrName = cm.getAttrName();
        cm.setCategory(Category.PRODUCT_SPEND);
        // initial values
        cm.setAttrState(AttrState.Active);
        if (systemAttributes.contains(cm.getAttrName())) {
            return cm;
        }
        cm.enableGroup(Segment);
        cm.disableGroup(Enrichment);
        if (ActivityMetricsUtils.isHasPurchasedAttr(attrName)) {
            cm.disableGroup(TalkingPoint);
        } else {
            cm.enableGroup(TalkingPoint);
        }
        cm.disableGroup(CompanyProfile);
        cm.disableGroup(Model);
        return cm;
    }

}