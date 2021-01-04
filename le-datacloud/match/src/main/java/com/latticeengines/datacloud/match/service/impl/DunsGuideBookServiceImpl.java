package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.entitymgr.DunsGuideBookEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.DunsGuideBookEntityMgrImpl;
import com.latticeengines.datacloud.match.exposed.service.DunsGuideBookService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;

@Component("dunsGuideBookService")
public class DunsGuideBookServiceImpl implements DunsGuideBookService {

    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookServiceImpl.class);

    private Map<String, DunsGuideBookEntityMgr> entityMgrMap = new ConcurrentHashMap<>(); // key is DataCloudVersion#getVersion

    @Inject
    private FabricDataService dataService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private DataCloudVersionService versionService;


    @Override
    public DunsGuideBook get(String version, String duns) {
        Preconditions.checkNotNull(version);
        Preconditions.checkNotNull(duns);

        String standardDuns = StringStandardizationUtils.getStandardDuns(duns);
        if (standardDuns == null) {
            return null;
        }

        DunsGuideBookEntityMgr mgr = getEntityMgr(version);
        return mgr.findByKey(standardDuns);
    }

    @Override
    public List<DunsGuideBook> get(String version, List<String> dunsList) {
        Preconditions.checkNotNull(version);
        check(dunsList);
        if (dunsList.isEmpty()) {
            return Collections.emptyList();
        }

        // Standard duns => Input duns
        Map<String, String> dunsMap = dunsList.stream()
                .map(duns -> Pair.of(duns, StringStandardizationUtils.getStandardDuns(duns)))
                // retain valid duns
                .filter(pair -> pair.getRight() != null)
                // ignore duplicate because the value should be the same
                .collect(Collectors.toMap(Pair::getValue, Pair::getKey, (v1, v2) -> v1));
        List<String> standardDunsList = new ArrayList<>(dunsMap.keySet());
        // Input duns => DunsGuideBook
        final Map<String, DunsGuideBook> bookMap = new HashMap<>();
        if (!standardDunsList.isEmpty()) {
            DunsGuideBookEntityMgr mgr = getEntityMgr(version);
            bookMap.putAll(mgr.batchFindByKey(standardDunsList)
                    .stream()
                    // resulting DunsGuideBooks
                    .filter(Objects::nonNull)
                    // [Input DUNS, DunsGuideBook]
                    .map(book -> Pair.of(dunsMap.get(book.getId()), book))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        }

        return dunsList.stream().map(bookMap::get).collect(Collectors.toList());
    }

    @Override
    public void set(String version, List<DunsGuideBook> books) {
        Preconditions.checkNotNull(version);
        Preconditions.checkNotNull(books);
        books = books.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (books.isEmpty()) {
            return;
        }

        DunsGuideBookEntityMgr mgr = getEntityMgr(version);
        mgr.batchCreate(books);
    }

    @Override
    public DunsGuideBookEntityMgr getEntityMgr(String version) {
        DunsGuideBookEntityMgr mgr = entityMgrMap.get(version);
        if (mgr == null) {
            mgr = getEntityMgrSync(version);
        }
        return mgr;
    }

    private synchronized DunsGuideBookEntityMgr getEntityMgrSync(@NotNull String version) {
        DunsGuideBookEntityMgr mgr = entityMgrMap.get(version);
        if (mgr == null) {
            String fullVersion = getFullDataCloudVersion(version);
            log.info("Use {} as full version of DunsGuideBook for {}", fullVersion, version);
            mgr = new DunsGuideBookEntityMgrImpl(dataService, fullVersion);
            mgr.init();
            entityMgrMap.put(version, mgr);
        }
        return mgr;
    }

    /*
     * if signature not empty
     *   fullVersion = version + "_" + signature
     * else
     *   fullVersion = version
     */
    private String getFullDataCloudVersion(@NotNull String version) {
        DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(version);
        // throw illegal argument exception if version not found (for consistency with existing class)
        Preconditions.checkArgument(dataCloudVersion != null,
                "Cannot find the specified data cloud version " + version);
        String signature = dataCloudVersion.getDynamoTableSignatureDunsGuideBook();
        return versionService.constructDynamoVersion(version, signature);
    }

    private void check(List<String> dunsList) {
        Preconditions.checkNotNull(dunsList);
        for (String duns : dunsList) {
            Preconditions.checkNotNull(duns);
        }
    }
}
