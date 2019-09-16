package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;

public interface ServingStoreProxy {
    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups, DataCollection.Version version);

    Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace);

    Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, DataCollection.Version version);

    // if not specified, default value for entity is Account.
    Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity,
            DataCollection.Version version);

    Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace);

    // allCustomerAttrs=TRUE is used for PA, in modeling, set it to false or
    // null
    Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, Boolean allCustomerAttrs,
            DataCollection.Version version);

    // if not specified, default value for entity is Account.
    Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, BusinessEntity entity, Boolean allCustomerAttrs,
            Version version);

    // only use cache when you have performance needs.
    // otherwise using above non-cached apis gives more up-to-date info.
    List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity);

    Set<String> getServingStoreColumnsFromCache(String customerSpace, BusinessEntity entity);

    List<ColumnMetadata> getDecoratedMetadata(String customerSpace, List<BusinessEntity> entities,
                                              List<ColumnSelection.Predefined> groups, DataCollection.Version version);
}
