package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component("accountMasterColumnSelectionService")
public class AccountMasterColumnSelectionServiceImpl implements ColumnSelectionService {

    private Logger log = LoggerFactory.getLogger(AccountMasterColumnSelectionServiceImpl.class);

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    /*
     * Both of following caches are loaded from base cache of metadata in
     * BaseMetadataColumnServiceImpl.
     *
     * Watching on zk node AMRelease which has dependency on AMReleaseBaseCache.
     * To trigger cache reload, first change zk node AMReleaseBaseCache, wait
     * for a short silent period after base cache finishes loading, then change
     * zk node AMRelease
     */

    // DataCloudVersion -> (<ColumnName -> EncodedColumnName>, <EncodedColumnName -> BitCodeBook>)
    private WatcherCache<String, Pair<Map<String, String>, Map<String, BitCodeBook>>> codeBookCache;
    // (DataCloudVersion, Predefined) -> ColumnSelection (consisted by a list of Column)
    private WatcherCache<ImmutablePair<String, Predefined>, ColumnSelection> predefinedSelectionCache;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @PostConstruct
    private void postConstruct() {
        initCaches();
    }

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public ColumnSelection parsePredefinedColumnSelection(Predefined predefined, String dataCloudVersion) {
        if (Predefined.supportedSelections.contains(predefined)) {
            if (StringUtils.isEmpty(dataCloudVersion)) {
                dataCloudVersion = getLatestVersion();
            }
            ColumnSelection selection = predefinedSelectionCache.get(ImmutablePair.of(dataCloudVersion, predefined));
            if (selection == null) {
                throw new RuntimeException(
                        "Cannot find selection " + predefined + " for version " + dataCloudVersion + " in cache.");
            }
            return selection;
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
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

        if (StringUtils.isEmpty(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        Pair<Map<String, String>, Map<String, BitCodeBook>> pair = codeBookCache.get(dataCloudVersion);
        if (pair == null) {
            throw new RuntimeException("Cannot find code book info for version " + dataCloudVersion + " in cache.");
        }
        Map<String, String> codeBookLookup = pair.getLeft();
        Map<String, BitCodeBook> codeBookMap = pair.getRight();
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
                    decodeFieldMap.put(encodedColumn, new ArrayList<>());
                }
                decodeFieldMap.get(encodedColumn).add(columnId);
            }
        }

        Map<String, Pair<BitCodeBook, List<String>>> toReturn = new HashMap<>(); // encodedColumn-><bitCodeBook,
                                                                                 // List<decodedColumn>>
        for (String encodedColumn : codeBooks) {
            BitCodeBook codeBook = codeBookMap.get(encodedColumn);
            List<String> decodeFields = decodeFieldMap.get(encodedColumn);
            toReturn.put(encodedColumn, Pair.of(codeBook, decodeFields));
        }

        return toReturn;
    }

    private void constructCodeBookMap(Map<String, BitCodeBook> codeBookMap, Map<String, String> codeBookLookup,
            String dataCloudVersion) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, Integer>> bitPosMap = new HashMap<>();
        Map<String, BitCodeBook.DecodeStrategy> decodeStrategyMap = new HashMap<>();
        Map<String, Map<String, Object>> valueDictRevMap = new HashMap<>();
        Map<String, Integer> bitUnitMap = new HashMap<>();
        List<AccountMasterColumn> columns = accountMasterColumnService.getMetadataColumns(dataCloudVersion);
        if (CollectionUtils.isNotEmpty(columns)) {
            for (AccountMasterColumn column : columns) {
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
                    bitPosMap.put(encodedColumn, new HashMap<>());
                }
                bitPosMap.get(encodedColumn).put(columnName, bitPos);
                if (!decodeStrategyMap.containsKey(encodedColumn)) {
                    String decodeStr = jsonNode.get("BitInterpretation").asText();
                    try {
                        BitCodeBook.DecodeStrategy decodeStrategy = BitCodeBook.DecodeStrategy.valueOf(decodeStr);
                        decodeStrategyMap.put(encodedColumn, decodeStrategy);
                        switch (decodeStrategy) {
                        case ENUM_STRING:
                            String valueDictStr = jsonNode.get("ValueDict").asText();
                            String[] valueDictArr = valueDictStr.split("\\|\\|");
                            Map<String, Object> valueDictRev = new HashMap<>();
                            for (int i = 0; i < valueDictArr.length; i++) {
                                valueDictRev.put(Integer.toBinaryString(i + 1), valueDictArr[i]);
                            }
                            valueDictRevMap.put(encodedColumn, valueDictRev);
                        case NUMERIC_INT:
                        case NUMERIC_UNSIGNED_INT:
                            Integer bitUnit = jsonNode.get("BitUnit").asInt();
                            bitUnitMap.put(encodedColumn, bitUnit);
                            break;
                        default:
                            break;
                        }
                    } catch (Exception e) {
                        log.error("Could not understand decode strategy", e);
                    }
                }
                if (codeBookLookup.containsKey(columnName)) {
                    throw new RuntimeException("Column " + columnName + " is already defined to use encoded column "
                            + codeBookLookup.get(columnName) + ", but now it is tried to use " + encodedColumn);
                }
                codeBookLookup.put(columnName, encodedColumn);
            }
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
            codeBook.setBitUnit(bitUnitMap.get(entry.getKey()));
            codeBook.setValueDictRev(valueDictRevMap.get(entry.getKey()));
            codeBookMap.put(entry.getKey(), codeBook);
        }
    }

    private ColumnSelection getPredefinedColumnSelection(Predefined selection, String dataCloudVersion) {
        List<AccountMasterColumn> externalColumns = accountMasterColumnService.findByColumnSelection(selection,
                dataCloudVersion);
        ColumnSelection cs = new ColumnSelection();
        cs.createAccountMasterColumnSelection(externalColumns);
        return cs;
    }

    @SuppressWarnings("unchecked")
    private void initCaches() {
        String currentApproved = versionEntityMgr.currentApprovedVersionAsString();
        List<ImmutablePair<String, Predefined>> predefinedForLatestVersion = Predefined.supportedSelections.stream() //
                .map(p -> ImmutablePair.of(currentApproved, p)).collect(Collectors.toList());
        ImmutablePair<String, Predefined>[] initKeys = predefinedForLatestVersion
                .toArray(new ImmutablePair[predefinedForLatestVersion.size()]);
        predefinedSelectionCache = WatcherCache.builder() //
                .name("PredefinedSelectionCache") //
                .watch(AMRelease.name()) //
                .maximum(100) //
                .load(key -> {
                    ImmutablePair<String, Predefined> pair = (ImmutablePair<String, Predefined>) key;
                    return getPredefinedColumnSelection(pair.getRight(), pair.getLeft());
                }) //
                .initKeys(initKeys) //
                // .waitBeforeRefreshInSec((int) (Math.random() * 30)) //
                .build();
        codeBookCache = WatcherCache.builder() //
                .name("CodeBookCache") //
                .watch(AMRelease.name()) //
                .maximum(20) //
                .load(version -> {
                    Map<String, BitCodeBook> codeBookMap = new HashMap<>();
                    Map<String, String> codeBookLookup = new HashMap<>();
                    constructCodeBookMap(codeBookMap, codeBookLookup, (String) version);
                    return ImmutablePair.of(codeBookLookup, codeBookMap);
                }) //
                .initKeys(new String[] { currentApproved }) //
                // .waitBeforeRefreshInSec((int) (Math.random() * 30)) //
                .build();
    }

    private String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersion().getVersion();
    }
};
