package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

import reactor.core.publisher.Flux;

public interface ServingStoreProxy {

    // ========== BEGIN: Get Metadata Not From Cache ==========
    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups, DataCollection.Version version);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups, DataCollection.Version version, StoreFilter filter);

    List<ColumnMetadata> getAccountMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version);

    List<ColumnMetadata> getContactMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version);
    // ========== END: Get Metadata Not From Cache ==========

    // ========== BEGIN: Get Metadata From Cache ==========
    // only use cache when you have performance needs.
    // otherwise using above non-cached apis gives more up-to-date info.
    List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity);

    Set<String> getServingStoreColumnsFromCache(String customerSpace, BusinessEntity entity);

    List<ColumnMetadata> getAccountMetadataFromCache(String customerSpace, ColumnSelection.Predefined group);
    // ========== END: Get Metadata From Cache ==========

    // ========== BEGIN: Modeling Attributes ==========
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
    // ========== END: Modeling Attributes ==========

}
