package com.latticeengines.apps.core.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

public class AttrTypeResolver {
    private static final Logger log = LoggerFactory.getLogger(AttrTypeResolver.class);

    private static Map<BusinessEntity, Set<String>> internalMap = new HashMap<>();
    private static Map<BusinessEntity, Set<String>> standardMap = new HashMap<>();

    static {
        BusinessEntity.SEGMENT_ENTITIES
                .forEach(e -> internalMap.put(e, SchemaRepository.getSystemAttributes(e).stream()
                        .map(InterfaceName::name).collect(Collectors.toSet())));
        BusinessEntity.SEGMENT_ENTITIES
                .forEach(e -> standardMap.put(e, SchemaRepository.getStandardAttributes(e).stream()
                        .map(InterfaceName::name).collect(Collectors.toSet())));
    }

    public static AttrType resolveType(ColumnMetadata metadata) {
        BusinessEntity entity = metadata.getEntity();
        AttrType type;
        if (entity != null && internalMap.get(entity) != null
                && internalMap.get(entity).contains(metadata.getAttrName())) {
            type = AttrType.Internal;
        } else if ((BusinessEntity.Account.equals(entity)
                || BusinessEntity.LatticeAccount.equals(entity))
                && !Category.ACCOUNT_ATTRIBUTES.equals(metadata.getCategory())) {
            type = AttrType.DataCloud;
        } else if (BusinessEntity.PurchaseHistory.equals(entity)
                || BusinessEntity.Rating.equals(entity)
                || BusinessEntity.CuratedAccount.equals(entity)) {
            type = AttrType.Curated;
        } else {
            type = AttrType.Custom;
        }
        return type;
    }

    public static AttrSubType resolveSubType(ColumnMetadata metadata) {
        AttrType type = resolveType(metadata);
        AttrSubType subType = null;
        BusinessEntity entity = metadata.getEntity();
        switch (type) {
            case Internal:
                break;
            case DataCloud:
                if (StringUtils.isNotBlank(metadata.getDataLicense())) {
                    subType = AttrSubType.Premium;
                } else if (Boolean.TRUE.equals(metadata.getCanInternalEnrich())) {
                    subType = AttrSubType.InternalEnrich;
                } else {
                    subType = AttrSubType.Normal;
                }
                break;
            case Curated:
                if (BusinessEntity.Rating.equals(entity)) {
                    subType = AttrSubType.Rating;
                } else if (BusinessEntity.PurchaseHistory.equals(entity)) {
                    String attrName = metadata.getAttrName();
                    if (ActivityMetricsUtils.isHasPurchasedAttr(attrName)) {
                        subType = AttrSubType.HasPurchased;
                    } else {
                        subType = AttrSubType.ProductBundle;
                    }
                } else if (BusinessEntity.CuratedAccount.equals(entity)) {
                    subType = AttrSubType.CuratedAccount;
                }
                break;
            case Custom:
                if (entity != null && standardMap.get(entity) != null
                        && standardMap.get(entity).contains(metadata.getAttrName())) {
                    subType = AttrSubType.Standard;
                } else if (metadata.isEnabledFor(ColumnSelection.Predefined.LookupId)) {
                    subType = AttrSubType.LookupId;
                } else {
                    subType = AttrSubType.Extension;
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported AttrType %s.", type));
        }
        return subType;
    }
}
