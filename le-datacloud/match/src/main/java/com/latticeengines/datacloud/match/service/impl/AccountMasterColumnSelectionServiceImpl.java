package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component("accountMasterColumnSelectionService")
public class AccountMasterColumnSelectionServiceImpl implements ColumnSelectionService {

    private Log log = LogFactory.getLog(AccountMasterColumnSelectionServiceImpl.class);

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    private ConcurrentMap<String, Object> accountMasterColumns = new ConcurrentHashMap<>();
    private static final String PREDEFINED_SELECTION_MAP = "PredefinedSelectionMap";
    private static final String COMLETE_CODEBOOK_CACHE = "CompleteCodeBookCache";
    private static final String CODEBOOK_LOOKUP = "CodeBookLookup";

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @PostConstruct
    private void postConstruct() {
        loadCaches();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCaches();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(17)), TimeUnit.MINUTES.toMillis(17));
    }

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ColumnSelection parsePredefinedColumnSelection(Predefined predefined, String dataCloudVersion) {
        if (Predefined.supportedSelections.contains(predefined)) {
            if (StringUtils.isEmpty(dataCloudVersion)) {
                dataCloudVersion = getLatestVersion();
            }
            if (((ConcurrentMap<String, ConcurrentMap<Predefined, ColumnSelection>>)accountMasterColumns.get(PREDEFINED_SELECTION_MAP)).containsKey(dataCloudVersion)) {
                return ((ConcurrentMap<String, ConcurrentMap<Predefined, ColumnSelection>>) accountMasterColumns
                        .get(PREDEFINED_SELECTION_MAP)).get(dataCloudVersion).get(predefined);
            } else {
                throw new RuntimeException(
                        "Cannot find selection " + predefined + " for version " + dataCloudVersion + " in cache.");
            }
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
    }

    @Override
    public List<String> getMatchedColumns(ColumnSelection selection) {
        return selection.getColumnIds();
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentVersion(Predefined predefined) {
        return "2.0";
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Pair<BitCodeBook, List<String>>> getDecodeParameters(ColumnSelection columnSelection,
            String dataCloudVersion) {

        if (StringUtils.isEmpty(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        Map<String, String> codeBookLookup = ((ConcurrentMap<String, ConcurrentMap<String, String>>) accountMasterColumns
                .get(CODEBOOK_LOOKUP)).get(dataCloudVersion);
        Map<String, BitCodeBook> codeBookMap = ((ConcurrentMap<String, ConcurrentMap<String, BitCodeBook>>) accountMasterColumns
                .get(COMLETE_CODEBOOK_CACHE)).get(dataCloudVersion);

        Set<String> codeBooks = new HashSet<>();
        Map<String, List<String>> decodeFieldMap = new HashMap<>();
        for (String columnId : columnSelection.getColumnIds()) {
            // This cannot handle user overwriting column name and assume it is
            // the same as ColumnID.
            // It is fine because there is no such use case for now

            if (codeBookLookup.containsKey(columnId)) {
                String encodedColumn = codeBookLookup.get(columnId);
                codeBooks.add(encodedColumn);
                if (!decodeFieldMap.containsKey(encodedColumn)) {
                    decodeFieldMap.put(encodedColumn, new ArrayList<String>());
                }
                decodeFieldMap.get(encodedColumn).add(columnId);
            }
        }

        Map<String, Pair<BitCodeBook, List<String>>> toReturn = new HashMap<>();
        for (String encodedClolumn : codeBooks) {
            BitCodeBook codeBook = codeBookMap.get(encodedClolumn);
            List<String> decodeFields = decodeFieldMap.get(encodedClolumn);
            toReturn.put(encodedClolumn, Pair.of(codeBook, decodeFields));
        }

        return toReturn;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, List<String>> getEncodedColumnMapping(ColumnSelection columnSelection,
            String dataCloudVersion) {

        if (StringUtils.isEmpty(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        Map<String, String> codeBookLookup = ((ConcurrentMap<String, ConcurrentMap<String, String>>) accountMasterColumns
                .get(CODEBOOK_LOOKUP)).get(dataCloudVersion);
        Map<String, BitCodeBook> codeBookMap = ((ConcurrentMap<String, ConcurrentMap<String, BitCodeBook>>) accountMasterColumns
                .get(COMLETE_CODEBOOK_CACHE)).get(dataCloudVersion);

        Map<String, List<String>> decodeFieldMap = new HashMap<>();
        for (String columnId : columnSelection.getColumnIds()) {
            // This cannot handle user overwriting column name and assume it is
            // the same as ColumnID.
            // It is fine because there is no such use case for now

            if (codeBookMap.containsKey(columnId)) {
                for(String decodedField: codeBookLookup.keySet()){
                    if(codeBookLookup.get(decodedField).equals(columnId)) {
                        if (!decodeFieldMap.containsKey(columnId)) {
                            decodeFieldMap.put(columnId, new ArrayList<String>());
                        }
                        decodeFieldMap.get(columnId).add(decodedField);
                    }
                }
            }
        }

        return decodeFieldMap;
    }

    private void constructCodeBookMap(Map<String, BitCodeBook> codeBookMap, Map<String, String> codeBookLookup,
            String dataCloudVersion) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, Integer>> bitPosMap = new HashMap<>();
        Map<String, BitCodeBook.DecodeStrategy> decodeStrategyMap = new HashMap<>();
        for (AccountMasterColumn column : accountMasterColumnService.scan(dataCloudVersion)) {
            String decodeStrategyStr = column.getDecodeStrategy();
            if (StringUtils.isEmpty(decodeStrategyStr)) {
                continue;
            }
            JsonNode jsonNode;
            try {
                jsonNode = objectMapper.readTree(decodeStrategyStr);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse decodeStrategy " + decodeStrategyStr);
            }
            String encodedColumn = jsonNode.has("EncodedColumn") ? jsonNode.get("EncodedColumn").asText() : null;
            String columnName = column.getColumnId();
            Integer bitPos = jsonNode.get("BitPosition").asInt();
            if (!bitPosMap.containsKey(encodedColumn)) {
                bitPosMap.put(encodedColumn, new HashMap<String, Integer>());
            }
            bitPosMap.get(encodedColumn).put(columnName, bitPos);
            if (!decodeStrategyMap.containsKey(encodedColumn)) {
                String decodeStr = jsonNode.get("BitInterpretation").asText();
                try {
                    BitCodeBook.DecodeStrategy decodeStrategy = BitCodeBook.DecodeStrategy.valueOf(decodeStr);
                    decodeStrategyMap.put(encodedColumn, decodeStrategy);
                } catch (Exception e) {
                    log.error("Could not understand decode strategy");
                }
            }
            if (codeBookLookup.containsKey(columnName)) {
                throw new RuntimeException("Column " + columnName + " is already defined to use encoded column "
                        + codeBookLookup.get(columnName) + ", but now it is tried to use " + encodedColumn);
            }
            codeBookLookup.put(columnName, encodedColumn);
        }

        for (Map.Entry<String, Map<String, Integer>> entry : bitPosMap.entrySet()) {
            if (!decodeStrategyMap.containsKey(entry.getKey())) {
                throw new RuntimeException(
                        "Could not find a valid decode strategy for encoded column " + entry.getKey());
            }
        }

        for (Map.Entry<String, BitCodeBook.DecodeStrategy> entry : decodeStrategyMap.entrySet()) {
            if (!bitPosMap.containsKey(entry.getKey())) {
                throw new RuntimeException(
                        "Could not find a valid bit position map for encoded column " + entry.getKey());
            }
            BitCodeBook codeBook = new BitCodeBook(entry.getValue());
            codeBook.setBitsPosMap(bitPosMap.get(entry.getKey()));
            codeBookMap.put(entry.getKey(), codeBook);
        }
    }

    private ColumnSelection getPredefinedColumnSelectionFromDb(Predefined selection, String dataCloudVersion) {
        List<AccountMasterColumn> externalColumns = accountMasterColumnService.findByColumnSelection(selection,
                dataCloudVersion);
        ColumnSelection cs = new ColumnSelection();
        cs.createAccountMasterColumnSelection(externalColumns);
        return cs;
    }
    
    @SuppressWarnings("unchecked")
    private void loadCacheForVersion(String version, ConcurrentMap<String, Object> accountMasterColumns) {
        if (!accountMasterColumns.containsKey(PREDEFINED_SELECTION_MAP)) {
            ConcurrentMap<String, ConcurrentMap<Predefined, ColumnSelection>> predefinedSelectionMap = new ConcurrentHashMap<>();
            accountMasterColumns.put(PREDEFINED_SELECTION_MAP, predefinedSelectionMap);
        }
        if (!accountMasterColumns.containsKey(COMLETE_CODEBOOK_CACHE)) {
            ConcurrentMap<String, ConcurrentMap<String, BitCodeBook>> completeCodeBookCache = new ConcurrentHashMap<>();
            accountMasterColumns.put(COMLETE_CODEBOOK_CACHE, completeCodeBookCache);
        }
        if (!accountMasterColumns.containsKey(CODEBOOK_LOOKUP)) {
            ConcurrentMap<String, ConcurrentMap<String, String>> codeBookLookup = new ConcurrentHashMap<>();
            accountMasterColumns.put(CODEBOOK_LOOKUP, codeBookLookup);
        }

        ConcurrentMap<String, ConcurrentMap<Predefined, ColumnSelection>> predefinedSelectionMap = ((ConcurrentMap<String, ConcurrentMap<Predefined, ColumnSelection>>) accountMasterColumns
                .get(PREDEFINED_SELECTION_MAP));
        ConcurrentMap<String, ConcurrentMap<String, BitCodeBook>> completeCodeBookCache = (ConcurrentMap<String, ConcurrentMap<String, BitCodeBook>>) accountMasterColumns
                .get(COMLETE_CODEBOOK_CACHE);
        ConcurrentMap<String, ConcurrentMap<String, String>> codeBookLookup = (ConcurrentMap<String, ConcurrentMap<String, String>>) accountMasterColumns
                .get(CODEBOOK_LOOKUP);

        predefinedSelectionMap.put(version, new ConcurrentHashMap<Predefined, ColumnSelection>());
        for (Predefined selection : Predefined.supportedSelections) {
            try {
                ColumnSelection cs = getPredefinedColumnSelectionFromDb(selection, version);
                predefinedSelectionMap.get(version).put(selection, cs);
            } catch (Exception e) {
                log.error(e);
            }
        }

        ConcurrentMap<String, BitCodeBook> newCodeBookMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, String> newCodeBookLookup = new ConcurrentHashMap<>();
        constructCodeBookMap(newCodeBookMap, newCodeBookLookup, version);
        completeCodeBookCache.put(version, newCodeBookMap);
        codeBookLookup.put(version, newCodeBookLookup);
        log.info("Loaded " + newCodeBookMap.size() + " bit code books for version " + version + " into cache.");
        log.info("Loaded " + newCodeBookLookup.size() + " columns in bit code lookup for version " + version);
    }

    private void loadCaches() {
        ConcurrentMap<String, Object> accountMasterColumnsBak = accountMasterColumns;
        try {
            ConcurrentMap<String, Object> accountMasterColumnsNew = new ConcurrentHashMap<>();
            List<String> cachedVersions = getAllVersions();
            for (String version : cachedVersions) {
                loadCacheForVersion(version, accountMasterColumnsNew);
            }
            accountMasterColumns = accountMasterColumnsNew;
        } catch (Exception e) {
            log.error(e);
            accountMasterColumns = accountMasterColumnsBak;
        }
    }

    private List<String> getAllVersions() {
        List<DataCloudVersion> dataCloudVersions = versionEntityMgr.allVerions();
        List<String> versions = new ArrayList<>();
        for (DataCloudVersion dataCloudVersion : dataCloudVersions) {
            versions.add(dataCloudVersion.getVersion());
        }
        return versions;
    }

    private String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersion().getVersion();
    }
}
