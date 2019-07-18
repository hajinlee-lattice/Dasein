package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ActivityMetricsDecoratorFac;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;


@Component("activityMetricsDecorator")
public class ActivityMetricsDecoratorFacImpl implements ActivityMetricsDecoratorFac {

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsDecoratorFacImpl.class);

    // Entity match enabled or not doesn't impact activity metrics metadata
    private static final Set<String> systemAttributes = SchemaRepository //
            .getSystemAttributes(BusinessEntity.DepivotedPurchaseHistory, false).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActivityMetricsProxy metricsProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();

        if (StringUtils.isNotBlank(tenantId)) {
            return new Decorator() {

                @Override
                public String getName() {
                    return "metric-attrs";
                }

                @Override
                public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
                    String customerSpace = CustomerSpace.parse(tenantId).toString();
                    Map<String, List<Product>> productMap = loadProductMap(customerSpace);
                    List<ActivityMetrics> metrics = getActivityMetrics(customerSpace);
                    return metadata.map(m -> {
                        return staticFilter(m, productMap, metrics);
                    });
                }

                @Override
                public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
                    String customerSpace = CustomerSpace.parse(tenantId).toString();
                    Map<String, List<Product>> productMap = loadProductMap(customerSpace);
                    List<ActivityMetrics> metrics = getActivityMetrics(customerSpace);
                    return metadata.map(m -> {
                        return staticFilter(m, productMap, metrics);
                    });
                }
            };
        } else {
            return new DummyDecorator();
        }

    }

    /**
     * Static logic applied to all tenants
     */
    private static ColumnMetadata staticFilter(ColumnMetadata cm, Map<String, List<Product>> productMap,
            List<ActivityMetrics> metrics) {
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
        return checkDeprecate(cm, productMap, metrics);
    }

    /**
     * Load product map from active table for table role ConsolidatedProduct
     *
     * @param customerSpace
     * @return ProductId -> [Products]
     */
    private Map<String, List<Product>> loadProductMap(String customerSpace) {
        Table productTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct);
        if (productTable == null) {
            log.warn("Fail to find active table with role {} for tenant {}", TableRoleInCollection.ConsolidatedProduct,
                    customerSpace);
            return null;
        }
        List<Product> productList = new ArrayList<>(ProductUtils.loadProducts(yarnConfiguration,
                productTable.getExtracts().get(0).getPath(), Arrays.asList(ProductType.Analytic.name()), null));
        log.info("Loaded product list from table {} for tenant {}", productTable.getName(), customerSpace);
        return ProductUtils.getProductMap(productList);
    }

    private List<ActivityMetrics> getActivityMetrics(String customerSpace) {
        List<ActivityMetrics> metrics = metricsProxy.getActivityMetrics(customerSpace,
                ActivityType.PurchaseHistory);
        if (metrics == null) {
            return Collections.emptyList();
        }
        return metrics;
    }

    private static ColumnMetadata checkDeprecate(ColumnMetadata cm, Map<String, List<Product>> productMap,
            List<ActivityMetrics> metrics) {
        if (!ActivityMetricsUtils.isActivityMetricsAttr(cm.getAttrName())) {
            return cm;
        }
        if (MapUtils.isEmpty(productMap)) {
            return cm;
        }
        String productId = ActivityMetricsUtils.getProductIdFromFullName(cm.getAttrName());
        if (StringUtils.isBlank(productId)) {
            log.warn("Cannot parse product id from attribute name " + cm.getAttrName());
            return cm;
        }
        List<Product> products = productMap.get(productId);
        if (CollectionUtils.isEmpty(products)) {
            log.warn("Cannot find product with ProductId {}", productId);
            return cm;
        }
        Product product = products.stream().filter(Objects::nonNull).findFirst().orElse(null);
        if (product == null) {
            log.warn("Cannot find product with ProductId {}", productId);
            return cm;
        }
        if (ProductStatus.Obsolete.name().equalsIgnoreCase(product.getProductStatus())) {
            return markDeprecate(cm);
        }
        if (ActivityMetricsUtils.isDeprecated(cm.getAttrName(), metrics)) {
            return markDeprecate(cm);
        }
        return cm;
    }

    private static ColumnMetadata markDeprecate(ColumnMetadata cm) {
        cm.setShouldDeprecate(true);
        // For legacy code:
        // Previously when deprecated attribute's metadata was generated in PA,
        // "Deprecated" is appended to display name instead of rendering in
        // metadata decorator
        // As metadata decorator will append "Deprecated" in display name too
        // when shouldDeprecate is true, remove duplicate "Deprecated" word in
        // display name
        if (cm.getDisplayName() != null && cm.getDisplayName().contains(" (Deprecated)")) {
            cm.setDisplayName(cm.getDisplayName().replace(" (Deprecated)", ""));
        }
        return cm;
    }

}
