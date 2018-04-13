package com.latticeengines.datacloud.dataflow.transformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

public abstract class ActivityMetricsBaseFlow<T extends TransformerConfig> extends ConfigurableFlowBase<T> {

    protected Map<InterfaceName, Class<?>> metricsClasses;
    protected Map<ActivityMetrics, FieldMetadata> metricsMetadata;
    protected Map<ActivityMetrics, Map<String, FieldMetadata>> pivotMetricsMetadata;

    protected Node imputeNullDepivoted(Node node) {
        for (Map.Entry<ActivityMetrics, FieldMetadata> ent : metricsMetadata.entrySet()) {
            ActivityMetrics m = ent.getKey();
            FieldMetadata fm = ent.getValue();
            String field = ActivityMetricsUtils.getNameWithPeriod(m);
            node = imputeNull(node, m, field, fm);
        }
        return node;
    }

    protected Node imputeNullPivoted(Node node, List<String> pivotVals) {
        for (Map.Entry<ActivityMetrics, Map<String, FieldMetadata>> ent : pivotMetricsMetadata.entrySet()) {
            ActivityMetrics m = ent.getKey();
            for (Map.Entry<String, FieldMetadata> e : ent.getValue().entrySet()) {
                FieldMetadata fm = e.getValue();
                String field = ActivityMetricsUtils.getFullName(ActivityMetricsUtils.getNameWithPeriod(m), e.getKey());
                node = imputeNull(node, m, field, fm);
            }
        }
        return node;
    }

    protected Node imputeNull(Node node, ActivityMetrics m, String field, FieldMetadata fm) {
        switch (m.getMetrics()) {
        case SpendChange:
            node = node.apply(String.format("%s == null  ? Integer.valueOf(0) : %s", field, field),
                    new FieldList(field), fm);
            break;
        case TotalSpendOvertime:
        case AvgSpendOvertime:
            node = node.apply(String.format("%s == null  ? Double.valueOf(0) : %s", field, field), new FieldList(field),
                    fm);
            break;
        case HasPurchased:
            node = node.apply(String.format("%s == null  ? Boolean.valueOf(false) : %s", field, field),
                    new FieldList(field), fm);
            break;
        default:
            break;
        }
        return node;
    }

    protected void prepareMetricsMetadata(List<ActivityMetrics> metrics, List<String> pivotVals) {
        metricsClasses = new HashMap<>();
        metricsClasses.put(InterfaceName.Margin, Integer.class);
        metricsClasses.put(InterfaceName.ShareOfWallet, Integer.class);
        metricsClasses.put(InterfaceName.SpendChange, Integer.class);
        metricsClasses.put(InterfaceName.TotalSpendOvertime, Double.class);
        metricsClasses.put(InterfaceName.AvgSpendOvertime, Double.class);
        metricsClasses.put(InterfaceName.HasPurchased, Boolean.class);

        metricsMetadata = new HashMap<>();
        metrics.forEach(m -> {
            metricsMetadata.put(m,
                    new FieldMetadata(ActivityMetricsUtils.getNameWithPeriod(m), metricsClasses.get(m.getMetrics())));
        });

        if (CollectionUtils.isNotEmpty(pivotVals)) {
            pivotMetricsMetadata = new HashMap<>();
            metrics.forEach(m -> {
                pivotMetricsMetadata.put(m, new HashMap<>());
                pivotVals.forEach(pv -> {
                    pivotMetricsMetadata.get(m).put(pv,
                            new FieldMetadata(
                                    ActivityMetricsUtils.getFullName(ActivityMetricsUtils.getNameWithPeriod(m), pv),
                                    metricsClasses.get(m.getMetrics())));
                });
            });
        }
    }

}
