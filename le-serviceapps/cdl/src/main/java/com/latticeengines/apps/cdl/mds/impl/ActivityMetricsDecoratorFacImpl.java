package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ActivityMetricsDecoratorFac;
import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.aws.s3.S3KeyFilter;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
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
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.ProductUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;


@Component("activityMetricsDecorator")
public class ActivityMetricsDecoratorFacImpl implements ActivityMetricsDecoratorFac {

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsDecoratorFacImpl.class);

    // Entity match enabled or not doesn't impact activity metrics metadata
    private static final Set<String> systemAttributes = SchemaRepository //
            .getSystemAttributes(BusinessEntity.DepivotedPurchaseHistory, false).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private ActivityMetricsService activityMetricsService;

    @Inject
    private S3Service s3Service;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

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
                    CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
                    Pair<Map<String, List<Product>>, List<ActivityMetrics>> result = getProductMapAndMetrics(customerSpace);
                    return metadata.map(m -> staticFilter(customerSpace.toString(), m, result.getLeft(), result.getRight()));
                }

                @Override
                public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
                    CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
                    Pair<Map<String, List<Product>>, List<ActivityMetrics>> result = getProductMapAndMetrics(customerSpace);
                    return metadata.map(m -> staticFilter(customerSpace.toString(), m, result.getLeft(), result.getRight()));
                }
            };
        } else {
            return new DummyDecorator();
        }
    }

    private Pair<Map<String, List<Product>>, List<ActivityMetrics>> getProductMapAndMetrics(CustomerSpace customerSpace) {
        Map<String, List<Product>> productMap = loadProductMap(customerSpace);
        List<ActivityMetrics> metrics = activityMetricsService.findWithType(ActivityType.PurchaseHistory);
        if (metrics == null) {
            metrics = Collections.emptyList();
        }
        Pair<Map<String, List<Product>>, List<ActivityMetrics>> result = Pair.of(productMap, metrics);
        return result;
    }

    /**
     * Static logic applied to all tenants
     */
    private static ColumnMetadata staticFilter(String customerSpace, ColumnMetadata cm,
                                               Map<String, List<Product>> productMap,
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
        return checkDeprecate(customerSpace, cm, productMap, metrics);
    }

    /**
     * Load product map from active product table for table role
     * ConsolidatedProduct
     *
     * @param customerSpace
     * @return ProductId -> [Products]
     */
    private Map<String, List<Product>> loadProductMap(CustomerSpace customerSpace) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
            MultiTenantContext.setTenant(tenant);
            Table productTable = dataCollectionService.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedProduct, null);
            if (productTable == null) {
                log.warn(
                        "Active product table with table role {} for tenant {} doesn't exist. "
                                + "Marking all the activity metrics attributes as deprecated.",
                        TableRoleInCollection.ConsolidatedProduct, customerSpace.toString());
                return Collections.emptyMap();
            }
            HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            List<Product> productList = retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt={}: Loading product list from table {} for tenant {}", (ctx.getRetryCount() + 1),
                            productTable.getName(), customerSpace);
                }

                String s3Prefix = pathBuilder.getS3AtlasTablePrefix(customerSpace.getTenantId(),
                        productTable.getName());
                Iterator<InputStream> streamIter = s3Service.getObjectStreamIterator(s3Bucket, s3Prefix,
                        new S3KeyFilter() {
                        });
                return ProductUtils.loadProducts(streamIter,
                        Arrays.asList(ProductType.Analytic.name()), null);
            });

            Map<String, List<Product>> productMap = ProductUtils.getProductMap(productList);
            timer.setTimerMessage(String.format("Loaded product map from table %s for tenant %s",
                    productTable.getName(), customerSpace));
            return productMap;
        }

    }

    private static ColumnMetadata checkDeprecate(String customerSpace, ColumnMetadata cm,
                                                 Map<String, List<Product>> productMap,
                                                 List<ActivityMetrics> metrics) {
        if (!ActivityMetricsUtils.isActivityMetricsAttr(cm.getAttrName())) {
            return cm;
        }
        String productId = ActivityMetricsUtils.getProductIdFromFullName(cm.getAttrName());
        if (StringUtils.isBlank(productId)) {
            log.warn("Cannot parse product id from attribute name {}. Marking {} as deprecated for tennant {}",
                    cm.getAttrName(), cm.getAttrName(), customerSpace);
            return markDeprecate(cm);
        }
        List<Product> products = productMap.get(productId);
        if (CollectionUtils.isEmpty(products)) {
            log.warn(
                    "Cannot find product with ProductId {} derived from attribute name {}. "
                            + "Marking {} as deprecated for tennant {}",
                    productId, cm.getAttrName(), cm.getAttrName(), customerSpace);
            return markDeprecate(cm);
        }
        Product product = products.stream().filter(Objects::nonNull).findFirst().orElse(null);
        if (product == null) {
            log.warn(
                    "Cannot find product with ProductId {} derived from attribute name {}. "
                            + "Marking {} as deprecated for tennant {}",
                    productId, cm.getAttrName(), cm.getAttrName(), customerSpace);
            return markDeprecate(cm);
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
