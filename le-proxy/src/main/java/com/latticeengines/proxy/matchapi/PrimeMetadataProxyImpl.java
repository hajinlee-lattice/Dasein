package com.latticeengines.proxy.matchapi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
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

    private Map<String, DataBlock> getContainerDataBlocks(DataBlockEntitlementContainer container) {
        List<String> blockIds = new ArrayList<>();
        Map<String, DataBlock> blockIdToDataBlock = new HashMap<>();

        for (DataBlockEntitlementContainer.Domain domain : container.getDomains()) {
            List<String> domains = domain.getRecordTypes().entrySet().stream().map(entry -> entry.getValue())
                    .map(blockList -> blockList.stream().map(block -> block.getBlockId()).collect(Collectors.toList()))
                    .collect(ArrayList::new, List::addAll, List::addAll);

            blockIds.addAll(domains);
        }

        List<DataBlock> dataBlocks = _self.getBlockElements(blockIds);

        for (DataBlock dataBlock : dataBlocks) {
            blockIdToDataBlock.put(dataBlock.getBlockId(), dataBlock);
        }

        return blockIdToDataBlock;
    }

    private DataBlockEntitlementContainer enrichContainerWithElements(DataBlockEntitlementContainer container,
            Map<String, DataBlock> blockIdToDataBlock) {
        List<DataBlockEntitlementContainer.Domain> enrichedDomains = new ArrayList<>();

        for (DataBlockEntitlementContainer.Domain domain : container.getDomains()) {
            Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> enrichedRecordTypes = new HashMap<>();
            for (Map.Entry<DataRecordType, List<DataBlockEntitlementContainer.Block>> entry : domain.getRecordTypes()
                    .entrySet()) {
                List<DataBlockEntitlementContainer.Block> enrichedBlocks = new ArrayList<>();

                for (DataBlockEntitlementContainer.Block block : entry.getValue()) {
                    List<DataBlock.Level> levels = blockIdToDataBlock.get(block.getBlockId()).getLevels();
                    enrichedBlocks.add(new DataBlockEntitlementContainer.Block(block, levels));
                }
                enrichedRecordTypes.put(entry.getKey(), enrichedBlocks);
            }
            enrichedDomains.add(new DataBlockEntitlementContainer.Domain(domain.getDomain(), enrichedRecordTypes));
        }

        return new DataBlockEntitlementContainer(enrichedDomains);
    }

    @Override
    public DataBlockEntitlementContainer enrichEntitlementContainerWithElements(
            DataBlockEntitlementContainer container) {
        Map<String, DataBlock> blockIdToDataBlock = getContainerDataBlocks(container);
        return enrichContainerWithElements(container, blockIdToDataBlock);
    }

    @Override
    public List<PrimeColumn> getPrimeColumns(List<String> elementIds) {
        String url = constructUrl("/columns?elementIds={elementIds}", //
                StringUtils.join(elementIds, ","));
        return getList("get prime columns", url, PrimeColumn.class);
    }

    @Override
    public Collection<String> getBlocksContainingElements(List<String> elementIds) {
        String url = constructUrl("/blocks-containing");
        List<?> list = post("get blocks containing elements", url, elementIds, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    @Override
    public List<PrimeColumn> getCandidateColumns() {
        return _self.getCandidateColumnsFromDistributedCache();
    }

    @Override
    public List<DataBlock> getBlockElements(List<String> blockIds) {
        List<DataBlock> blockList = _self.getBlockElementsFromDistributedCache();
        if (CollectionUtils.isNotEmpty(blockIds)) {
            return blockList.stream().filter(block -> blockIds.contains(block.getBlockId()))
                    .collect(Collectors.toList());
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
