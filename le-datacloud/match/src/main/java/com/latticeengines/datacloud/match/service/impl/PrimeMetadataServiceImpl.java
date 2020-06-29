package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.datacloud.match.repository.reader.DataBlockDomainEntitlementRepository;
import com.latticeengines.datacloud.match.repository.reader.DataBlockElementRepository;
import com.latticeengines.datacloud.match.repository.reader.DataBlockLevelMetadataRepository;
import com.latticeengines.datacloud.match.repository.reader.PrimeColumnRepository;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockDomainEntitlement;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevelMetadata;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

@Service
public class PrimeMetadataServiceImpl implements PrimeMetadataService {

    @Inject
    private PrimeColumnRepository primeColumnRepository;

    @Inject
    private DataBlockElementRepository dataBlockElementRepository;

    @Inject
    private DataBlockLevelMetadataRepository levelMetadataRepository;

    @Inject
    private DataBlockDomainEntitlementRepository entitlementRepository;

    @Override
    public DataBlockMetadataContainer getDataBlockMetadata() {
        String msg = "Fetch and construct data block metadata container.";
        try (PerformanceTimer time = new PerformanceTimer(msg)) {
            List<DataBlockLevelMetadata> metadataList = levelMetadataRepository.findAll();
            Map<String, Map<DataBlockLevel, DataBlockLevelMetadata>> blockMap = new HashMap<>();
            for (DataBlockLevelMetadata levelMetadata: metadataList) {
                String block = levelMetadata.getBlock();
                DataBlockLevel level = levelMetadata.getLevel();
                Map<DataBlockLevel, DataBlockLevelMetadata> levelMap = blockMap.getOrDefault(block, new HashMap<>());
                levelMap.put(level, levelMetadata);
                blockMap.put(block, levelMap);
            }
            List<DataBlock> blocks = new ArrayList<>();
            for (String block: blockMap.keySet()) {
                Map<DataBlockLevel, DataBlockLevelMetadata> levelMap = blockMap.get(block);
                List<DataBlock.Level> levels = new ArrayList<>();
                for (DataBlockLevel level: levelMap.keySet()) {
                    DataBlockLevelMetadata levelMetadata = levelMap.get(level);
                    levels.add(new DataBlock.Level(level, levelMetadata.getDescription()));
                }
                levels.sort(Comparator.comparing(DataBlock.Level::getLevel));
                blocks.add(new DataBlock(block, levels));
            }
            DataBlockMetadataContainer container = new DataBlockMetadataContainer();
            container.setBlocks(blocks.stream().collect(Collectors.toMap(DataBlock::getBlockId, Function.identity())));
            return container;
        }
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        try (PerformanceTimer time = new PerformanceTimer("Fetch and construct full data block tree.")) {
            List<DataBlockLevelMetadata> metadataList = levelMetadataRepository.findAll();
            Map<String, DataBlockLevelMetadata> levelMetadataMap = metadataList.stream() //
                    .collect(Collectors.toMap(DataBlockLevelMetadata::getFqBlockId, Function.identity()));
            Map<String, PrimeColumn> primeColumns = primeColumnRepository.findAll().stream() //
                    .collect(Collectors.toMap(PrimeColumn::getPrimeColumnId, Function.identity()));
            List<Object[]> queryResults = dataBlockElementRepository.getAllBlockElements();
            Map<String, Map<DataBlockLevel, List<PrimeColumn>>> blockMap = new HashMap<>();
            for (Object[] row: queryResults) {
                String block = (String) row[0];
                DataBlockLevel level = (DataBlockLevel) row[1];
                String columnId = (String) row[2];
                Map<DataBlockLevel, List<PrimeColumn>> levelMap = blockMap.getOrDefault(block, new HashMap<>());
                List<PrimeColumn> columnList = levelMap.getOrDefault(level, new ArrayList<>());
                if (primeColumns.get(columnId) == null) {
                    throw new NoSuchElementException("No prime column with id " + columnId);
                }
                columnList.add(primeColumns.get(columnId));
                levelMap.put(level, columnList);
                blockMap.put(block, levelMap);
            }
            List<DataBlock> blocks = new ArrayList<>();
            for (String block: blockMap.keySet()) {
                Map<DataBlockLevel, List<PrimeColumn>> levelMap = blockMap.get(block);
                List<DataBlock.Level> levels = new ArrayList<>();
                for (DataBlockLevel level: levelMap.keySet()) {
                    String fqBlockId = String.format("%s_%s", block, level);
                    if (levelMetadataMap.containsKey(fqBlockId)) {
                        DataBlockLevelMetadata levelMetadata = levelMetadataMap.get(fqBlockId);
                        List<PrimeColumn> columnList = levelMap.get(level);
                        List<DataBlock.Element> elements = columnList.stream() //
                                .map(DataBlock.Element::new).collect(Collectors.toList());
                        levels.add(new DataBlock.Level(level, levelMetadata.getDescription(), elements));
                    }
                }
                if (CollectionUtils.isNotEmpty(levels)) {
                    levels.sort(Comparator.comparing(DataBlock.Level::getLevel));
                    blocks.add(new DataBlock(block, levels));
                }
            }
            blocks.sort(Comparator.comparing(DataBlock::getBlockId));
            return blocks;
        }
    }

    @Override
    public DataBlockEntitlementContainer getBaseEntitlement() {
        String msg = "Fetch and construct data block metadata container.";
        try (PerformanceTimer time = new PerformanceTimer(msg)) {
            List<DataBlockDomainEntitlement> entitlementList = entitlementRepository.findAll();
            Map<DataDomain, Map<DataRecordType, Map<String, List<DataBlockLevel>>>> domainMap = new HashMap<>();
            for (DataBlockDomainEntitlement entitlement: entitlementList) {
                DataDomain domain = entitlement.getDomain();
                DataRecordType recordType = entitlement.getRecordType();
                DataBlockLevelMetadata levelMetadata = entitlement.getDataBlockLevel();
                String block = levelMetadata.getBlock();
                DataBlockLevel level = levelMetadata.getLevel();
                Map<DataRecordType, Map<String, List<DataBlockLevel>>> recordTypeMap = //
                        domainMap.getOrDefault(domain, new HashMap<>());
                Map<String, List<DataBlockLevel>> blockMap = recordTypeMap.getOrDefault(recordType, new HashMap<>());
                List<DataBlockLevel> levelList = blockMap.getOrDefault(block, new ArrayList<>());
                levelList.add(level);
                blockMap.put(block, levelList);
                recordTypeMap.put(recordType, blockMap);
                domainMap.put(domain, recordTypeMap);
            }
            List<DataBlockEntitlementContainer.Domain> domains = new ArrayList<>();
            for (DataDomain domain: domainMap.keySet()) {
                Map<DataRecordType, Map<String, List<DataBlockLevel>>> recordTypeMap = domainMap.get(domain);
                Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> recordTypes = new TreeMap<>();
                for (DataRecordType recordType: recordTypeMap.keySet()) {
                    Map<String, List<DataBlockLevel>> blockMap = recordTypeMap.get(recordType);
                    List<DataBlockEntitlementContainer.Block> blocks = new ArrayList<>();
                    for (String blockId: blockMap.keySet()) {
                        blocks.add(new DataBlockEntitlementContainer.Block(blockId, blockMap.get(blockId)));
                    }
                    blocks.sort(Comparator.comparing(DataBlockEntitlementContainer.Block::getBlockId));
                    recordTypes.put(recordType, blocks);
                }
                domains.add(new DataBlockEntitlementContainer.Domain(domain, recordTypes));
            }
            domains.sort(Comparator.comparing(domain -> domain.getDomain().ordinal()));
            return new DataBlockEntitlementContainer(domains);
        }
    }

}
