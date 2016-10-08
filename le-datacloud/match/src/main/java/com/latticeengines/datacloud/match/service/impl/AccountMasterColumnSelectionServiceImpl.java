package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.ArrayList;
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
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.propdata.core.entitymgr.DataCloudVersionEntityMgr;

@Component("accountMasterColumnSelectionService")
public class AccountMasterColumnSelectionServiceImpl implements ColumnSelectionService {

    private Log log = LogFactory.getLog(AccountMasterColumnSelectionServiceImpl.class);

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    private ConcurrentMap<Predefined, ColumnSelection> predefinedSelectionMap = new ConcurrentHashMap<>();

    private Map<String, BitCodeBook> completeCodeBookCache;
    private Map<String, String> codeBookLookup;
    private String cachedVersion = "";

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
        }, TimeUnit.MINUTES.toMillis(10));
    }

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public ColumnSelection parsePredefinedColumnSelection(Predefined predefined) {
        if (Predefined.supportedSelections.contains(predefined)) {
            return predefinedSelectionMap.get(predefined);
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
    }

    @Override
    public List<String> getMatchedColumns(ColumnSelection selection) {
        List<String> columnNames = new ArrayList<>();
        for (Column column : selection.getColumns()) {
            AccountMasterColumn externalColumn = accountMasterColumnService
                    .getMetadataColumn(column.getExternalColumnId(), getCurrentVersion(null));
            if (externalColumn != null) {
                columnNames.add(externalColumn.getAmColumnId());
            } else {
                columnNames.add(column.getColumnName());
            }
        }
        return columnNames;
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentVersion(Predefined predefined) {
        return "2.0";
    }

    @Override
    public Map<String, Pair<BitCodeBook, List<String>>> getDecodeParameters(ColumnSelection columnSelection,
            String dataCloudVersion) {

        // TODO: after implementing minor version, should check if dataCloudVersion is the same as cachedVersion

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
            BitCodeBook codeBook = completeCodeBookCache.get(encodedClolumn);
            List<String> decodeFields = decodeFieldMap.get(encodedClolumn);
            toReturn.put(encodedClolumn, Pair.of(codeBook, decodeFields));
        }

        return toReturn;
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

    private void loadCaches() {
        String cachedVersion = versionEntityMgr.latestApprovedForMajorVersion("2.0").getVersion();

        for (Predefined selection : Predefined.supportedSelections) {
            try {
                List<AccountMasterColumn> externalColumns = accountMasterColumnService.findByColumnSelection(selection, cachedVersion);
                ColumnSelection cs = new ColumnSelection();
                cs.createAccountMasterColumnSelection(externalColumns);
                predefinedSelectionMap.put(selection, cs);
            } catch (Exception e) {
                log.error(e);
            }
        }

        Map<String, BitCodeBook> codeBookMapBak = completeCodeBookCache;
        Map<String, String> codeBookLookupBak = codeBookLookup;
        try {
            Map<String, BitCodeBook> newCodeBookMap = new HashMap<>();
            Map<String, String> newCodeBookLookup = new HashMap<>();
            constructCodeBookMap(newCodeBookMap, newCodeBookLookup, cachedVersion);
            completeCodeBookCache = newCodeBookMap;
            codeBookLookup = newCodeBookLookup;
            log.info("Loaded " + completeCodeBookCache.size() + " bit code books in cache.");
            log.info("Loaded " + codeBookLookup.size() + " columns in bit code lookup.");
        } catch (Exception e) {
            log.error(e);
            completeCodeBookCache = codeBookMapBak;
            codeBookLookup = codeBookLookupBak;
        }

        this.cachedVersion = cachedVersion;
    }
}
