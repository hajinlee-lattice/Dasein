package com.latticeengines.datacloud.dataflow.transformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

abstract class ActivityMetricsBaseFlow<T extends TransformerConfig> extends ConfigurableFlowBase<T> {

    Map<ActivityMetrics, FieldMetadata> metricsMetadata;

    private Map<InterfaceName, Class<?>> metricsClasses;
    private Map<ActivityMetrics, Map<String, FieldMetadata>> pivotMetricsMetadata;

    void prepareMetricsMetadata(List<ActivityMetrics> metrics, List<String> pivotVals) {
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
