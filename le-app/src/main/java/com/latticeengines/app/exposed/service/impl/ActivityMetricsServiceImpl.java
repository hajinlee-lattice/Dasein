package com.latticeengines.app.exposed.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.ActivityMetricsService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetricsValidation;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Component("activityMetricsService")
public class ActivityMetricsServiceImpl implements ActivityMetricsService {
    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsServiceImpl.class);

    private final ServingStoreProxy servingStoreProxy;

    @Inject
    public ActivityMetricsServiceImpl(ServingStoreProxy servingStoreProxy) {
        this.servingStoreProxy = servingStoreProxy;
    }

    @Override
    public ActivityMetricsValidation validateActivityMetrics(String customerSpace) {
        ActivityMetricsValidation validation = new ActivityMetricsValidation();
        validation.setDisableAll(true);
        validation.setDisableShareOfWallet(true);
        validation.setDisableMargin(true);

        List<ColumnMetadata> prodCMList = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.Product);
        List<ColumnMetadata> trxCMList = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.PeriodTransaction);
        List<ColumnMetadata> acctCMList = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.Account);

        if (CollectionUtils.isNotEmpty(prodCMList) && CollectionUtils.isNotEmpty(trxCMList) &&
                CollectionUtils.isNotEmpty(acctCMList)) {
            validation.setDisableAll(false);
        } else {
            return validation;
        }

        for (ColumnMetadata metadata : trxCMList) {
            if (metadata.getAttrName().equalsIgnoreCase(InterfaceName.TotalCost.name())) {
                validation.setDisableMargin(false);
                break;
            }
        }

        for (ColumnMetadata metadata : acctCMList) {
            if (metadata.getAttrName().equalsIgnoreCase(InterfaceName.SpendAnalyticsSegment.name())) {
                validation.setDisableShareOfWallet(false);
                break;
            }
        }

        return validation;
    }
}
