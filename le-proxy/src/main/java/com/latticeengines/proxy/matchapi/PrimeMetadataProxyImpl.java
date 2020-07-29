package com.latticeengines.proxy.matchapi;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;

@Lazy
@Component
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class PrimeMetadataProxyImpl extends BaseRestApiProxy implements PrimeMetadataProxy {

    private final PrimeMetadataProxyImpl _self;

    public PrimeMetadataProxyImpl(PrimeMetadataProxyImpl _self) {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/prime-metadata");
        this._self = _self;
    }

    @Override
    public List<PrimeColumn> getPrimeColumns(List<String> elementIds) {
        String url = constructUrl("/columns?elementIds={elementIds}", //
                StringUtils.join(elementIds, ","));
        return getList("get prime columns", url, PrimeColumn.class);
    }

    @Override
    public List<PrimeColumn> getCandidateColumns() {
        return _self.getCandidateColumnsFromDistributedCache();
    }

    @Override
    public List<DataBlock> getBlockElements(List<String> blockIds) {
        List<DataBlock> blockList = _self.getBlockElementsFromDistributedCache();
        if (CollectionUtils.isNotEmpty(blockIds)) {
            return blockList.stream().filter(block -> blockIds.contains(block.getBlockId())).collect(Collectors.toList());
        } else {
            return blockList;
        }
    }

    @Override
    public DataBlockMetadataContainer getBlockMetadata() {
        return _self.getBlockMetadataFromDistributedCache();
    }

    @Cacheable(cacheNames = CacheName.Constants.PrimeMetadataCacheName, key = "T(java.lang.String).format(\"prime_elements\")", unless = "#result == null")
    public List<DataBlock> getBlockElementsFromDistributedCache() {
        String url = constructUrl("/elements");
        @SuppressWarnings("unchecked")
        List<DataBlock> blockList = getKryo("get block elements", url, List.class);
        return blockList;
    }

    @Cacheable(cacheNames = CacheName.Constants.PrimeMetadataCacheName, key = "T(java.lang.String).format(\"prime_blocks\")", unless = "#result == null")
    public DataBlockMetadataContainer getBlockMetadataFromDistributedCache() {
        String url = constructUrl("/blocks");
        return get("get block metadata", url, DataBlockMetadataContainer.class);
    }

    @SuppressWarnings("unchecked")
    @Cacheable(cacheNames = CacheName.Constants.PrimeMetadataCacheName, key = "T(java.lang.String).format(\"candidate_columns\")", unless = "#result == null")
    public List<PrimeColumn> getCandidateColumnsFromDistributedCache() {
        String url = constructUrl("/candidate-columns");
        return getKryo("get candidate columns", url, List.class);
    }

}
