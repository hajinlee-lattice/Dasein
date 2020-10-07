package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_BASE_INFO;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_ENTITY_RESOLUTION;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.datacloud.match.repository.reader.DataBlockElementRepository;
import com.latticeengines.datacloud.match.repository.reader.DataBlockLevelMetadataRepository;
import com.latticeengines.datacloud.match.repository.reader.PrimeColumnRepository;
import com.latticeengines.datacloud.match.service.DirectPlusCandidateService;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevelMetadata;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
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
    private DirectPlusCandidateService candidateService;

    @Override
    public DataBlockMetadataContainer getDataBlockMetadata() {
        String msg = "Fetch and construct data block metadata container.";
        try (PerformanceTimer time = new PerformanceTimer(msg)) {
            List<DataBlockLevelMetadata> metadataList = levelMetadataRepository.findAll();
            Map<String, Map<DataBlockLevel, DataBlockLevelMetadata>> blockMap = new HashMap<>();
            for (DataBlockLevelMetadata levelMetadata : metadataList) {
                String block = levelMetadata.getBlock();
                DataBlockLevel level = levelMetadata.getLevel();
                Map<DataBlockLevel, DataBlockLevelMetadata> levelMap = blockMap.getOrDefault(block, new HashMap<>());
                levelMap.put(level, levelMetadata);
                blockMap.put(block, levelMap);
            }
            List<DataBlock> blocks = new ArrayList<>();
            for (String block : blockMap.keySet()) {
                Map<DataBlockLevel, DataBlockLevelMetadata> levelMap = blockMap.get(block);
                List<DataBlock.Level> levels = new ArrayList<>();
                for (DataBlockLevel level : levelMap.keySet()) {
                    levels.add(new DataBlock.Level(level));
                }
                levels.sort(Comparator.comparing(DataBlock.Level::getLevel));
                blocks.add(new DataBlock(block, levels));
            }
            blocks.add(getBaseInfoBlock(false));
            blocks.add(getEntityResolutionBlock(false));
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
            for (Object[] row : queryResults) {
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
            for (String block : blockMap.keySet()) {
                Map<DataBlockLevel, List<PrimeColumn>> levelMap = blockMap.get(block);
                List<DataBlock.Level> levels = new ArrayList<>();
                for (DataBlockLevel level : levelMap.keySet()) {
                    String fqBlockId = String.format("%s_%s", block, level);
                    if (levelMetadataMap.containsKey(fqBlockId)) {
                        DataBlockLevelMetadata levelMetadata = levelMetadataMap.get(fqBlockId);
                        List<PrimeColumn> columnList = levelMap.get(level);
                        List<DataBlock.Element> elements = columnList.stream() //
                                .map(DataBlock.Element::new).collect(Collectors.toList());
                        levels.add(new DataBlock.Level(level, elements));
                    }
                }
                if (CollectionUtils.isNotEmpty(levels)) {
                    levels.sort(Comparator.comparing(DataBlock.Level::getLevel));
                    blocks.add(new DataBlock(block, levels));
                }
            }
            blocks.add(getBaseInfoBlock(true));
            blocks.add(getEntityResolutionBlock(true));
            blocks.sort(Comparator.comparing(DataBlock::getBlockId));
            return filterFinancialDataBlockLevels(blocks);
        }
    }

    @Override
    public List<PrimeColumn> getPrimeColumns(Collection<String> elementIds) {
        return primeColumnRepository.findAllByPrimeColumnIdIn(elementIds);
    }

    /**
     * Find minimum and lowest level blocks containing all elements requested
     */
    @Override
    public Set<String> getBlocksContainingElements(Collection<String> elementIds) {
        List<DataBlockElement> blockElements = //
                dataBlockElementRepository.findAllByPrimeColumn_PrimeColumnIdIn(elementIds);
        Map<String, List<String>> blockIds = consolidateBlocks(blockElements);
        Set<String> toReturn = blockIds.keySet().stream().filter(block -> !block.startsWith("baseinfo"))
                .collect(Collectors.toSet());
        if (toReturn.isEmpty()) {
            return Collections.singleton("companyinfo_L1_v1");
        } else {
            return toReturn;
        }
    }

    /**
     * Find minimum and lowest level blocks containing all elements requested
     */
    @Override
    public Map<String, List<PrimeColumn>> divideIntoBlocks(Collection<PrimeColumn> primeColumns) {
        Map<String, PrimeColumn> idToPcMap = primeColumns.stream() //
                .collect(Collectors.toMap(PrimeColumn::getPrimeColumnId, Function.identity()));
        List<DataBlockElement> blockElements = //
                dataBlockElementRepository.findAllByPrimeColumn_PrimeColumnIdIn(idToPcMap.keySet());
        Map<String, List<String>> blockIds = consolidateBlocks(blockElements);
        Map<String, List<PrimeColumn>> toReturn = new HashMap<>();
        blockIds.forEach((blockId, elementIdsInBlock) -> {
            List<PrimeColumn> primeColumnsInBlock = elementIdsInBlock.stream() //
                    .map(idToPcMap::get).collect(Collectors.toList());
            toReturn.put(blockId, primeColumnsInBlock);
        });
        return toReturn;
    }

    static List<DataBlock> filterFinancialDataBlockLevels(List<DataBlock> dataBlocks) {
        List<DataBlock> filteredDataBlocks = new ArrayList<>();

        for (DataBlock dataBlock : dataBlocks) {
            if (DataBlock.Id.companyfinancials.equals(dataBlock.getBlockId())) {
                List<DataBlock.Level> filteredLevels = new ArrayList<>();

                for (DataBlock.Level level : dataBlock.getLevels()) {
                    if (DataBlockLevel.L1.equals(level.getLevel())) {
                        filteredLevels.add(level);
                    }
                }

                filteredDataBlocks.add(new DataBlock(dataBlock.getBlockId(), filteredLevels));
            } else {
                filteredDataBlocks.add(dataBlock);
            }
        }

        return filteredDataBlocks;
    }

    // blockId -> list(elementId)
    static Map<String, List<String>> consolidateBlocks(Collection<DataBlockElement> blockElements) {
        // elementId -> list(dbe)
        Map<String, List<DataBlockElement>> elementIdToBlockElementMap = new HashMap<>();
        // blockId -> list(dbe) : result
        Map<String, List<DataBlockElement>> blockIdToBlockElementMap = new HashMap<>();

        for (DataBlockElement blockElement : blockElements) {
            String elmentId = blockElement.getPrimeColumn().getPrimeColumnId();
            List<DataBlockElement> blockElementsForElement = //
                    elementIdToBlockElementMap.getOrDefault(elmentId, new ArrayList<>());
            blockElementsForElement.add(blockElement);
            elementIdToBlockElementMap.put(elmentId, blockElementsForElement);
        }

        String maxCoverageBlockId;
        do {
            // elements can only be found in one block
            Set<String> singleChoiceBlocks;
            do {
                singleChoiceBlocks = new HashSet<>();
                for (String elementId : elementIdToBlockElementMap.keySet()) {
                    if (elementIdToBlockElementMap.get(elementId).size() == 1) {
                        String blockId = elementIdToBlockElementMap.get(elementId).get(0).getBlock();
                        singleChoiceBlocks.add(blockId);
                    }
                }
                for (String blockId : singleChoiceBlocks) {
                    takeBlock(blockId, elementIdToBlockElementMap, blockIdToBlockElementMap);
                }
            } while (!singleChoiceBlocks.isEmpty());
            Map<String, Integer> blockCoverageMap = new HashMap<>();
            for (String elementId : elementIdToBlockElementMap.keySet()) {
                for (DataBlockElement dbe : elementIdToBlockElementMap.get(elementId)) {
                    String blockId = dbe.getBlock();
                    int cnt = blockCoverageMap.getOrDefault(blockId, 0);
                    blockCoverageMap.put(blockId, cnt + 1);
                }
            }
            maxCoverageBlockId = blockCoverageMap.entrySet().stream() //
                    .min(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("");
            if (StringUtils.isNotBlank(maxCoverageBlockId)) {
                takeBlock(maxCoverageBlockId, elementIdToBlockElementMap, blockIdToBlockElementMap);
            }
        } while (StringUtils.isNotBlank(maxCoverageBlockId));
        // blockId -> list(elementId)
        Map<String, List<String>> blockElementMap = new HashMap<>();
        blockIdToBlockElementMap.values().forEach(dataBlockElements -> {
            DataBlockElement maxLevel = dataBlockElements.stream() //
                    .max(Comparator.comparing(DataBlockElement::getLevel)).orElse(null);
            Preconditions.checkNotNull(maxLevel);
            String blockId = maxLevel.getFqBlockId();
            List<String> elementIds = dataBlockElements.stream() //
                    .map(dbe -> dbe.getPrimeColumn().getPrimeColumnId()).collect(Collectors.toList());
            blockElementMap.put(blockId, elementIds);
        });
        return blockElementMap;
    }

    private static void takeBlock(String blockId, Map<String, //
            List<DataBlockElement>> elementIdToBlockElementMap, //
            Map<String, List<DataBlockElement>> blockIdToBlockElementMap) {
        List<DataBlockElement> elements = blockIdToBlockElementMap.getOrDefault(blockId, new ArrayList<>());
        Set<String> elementIdsToRemove = new HashSet<>();
        for (String elementId : elementIdToBlockElementMap.keySet()) {
            List<DataBlockElement> remainingDbeList = new ArrayList<>();
            boolean belongToBlock = false;
            for (DataBlockElement dbe : elementIdToBlockElementMap.get(elementId)) {
                if (blockId.equals(dbe.getBlock())) {
                    elements.add(dbe);
                    belongToBlock = true;
                } else {
                    remainingDbeList.add(dbe);
                }
            }
            if (belongToBlock || remainingDbeList.isEmpty()) {
                elementIdsToRemove.add(elementId);
            } else {
                elementIdToBlockElementMap.put(elementId, remainingDbeList);
            }
        }
        elementIdsToRemove.forEach(elementIdToBlockElementMap::remove);
        blockIdToBlockElementMap.put(blockId, elements);
    }

    private DataBlock getBaseInfoBlock(boolean includeElements) {
        DataBlock.Level level;
        if (includeElements) {
            List<PrimeColumn> primeColumns = primeColumnRepository.findAllByDataBlocks_Block("baseinfo");
            List<DataBlock.Element> elements = primeColumns.stream() //
                    .map(DataBlock.Element::new).collect(Collectors.toList());
            level = new DataBlock.Level(DataBlockLevel.L1, elements);
        } else {
            level = new DataBlock.Level(DataBlockLevel.L1);
        }
        return new DataBlock(BLOCK_BASE_INFO, Collections.singleton(level));
    }

    private DataBlock getEntityResolutionBlock(boolean includeElements) {
        DataBlock.Level level;
        if (includeElements) {
            List<PrimeColumn> primeColumns = candidateService.candidateColumns();
            List<DataBlock.Element> elements = primeColumns.stream() //
                    .map(DataBlock.Element::new).collect(Collectors.toList());
            level = new DataBlock.Level(DataBlockLevel.L1, elements);
        } else {
            level = new DataBlock.Level(DataBlockLevel.L1);
        }
        return new DataBlock(BLOCK_ENTITY_RESOLUTION, Collections.singleton(level));
    }

}
