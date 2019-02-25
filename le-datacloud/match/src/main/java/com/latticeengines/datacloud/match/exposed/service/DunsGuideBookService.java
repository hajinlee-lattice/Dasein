package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.entitymgr.DunsGuideBookEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;

/**
 * Manage multiple {@link DunsGuideBookEntityMgr} that are associated with different {@link DataCloudVersion#getVersion()}.
 */
public interface DunsGuideBookService {
    /**
     * Retrieve the {@link DunsGuideBook} for the given DUNS under specified datacloud version
     * @param version target {@link DataCloudVersion#getVersion()}, has to be a valid version
     * @param duns target DUNS, will be standardized
     * @return associated {@link DunsGuideBook}, {@literal null} if guide book does not exist or the DUNS is not valid
     */
    DunsGuideBook get(@NotNull String version, @NotNull String duns);

    /**
     * Retrieve a list of {@link DunsGuideBook} for a given list of DUNS respectively.
     * @param version target {@link DataCloudVersion#getVersion()}, has to be a valid version
     * @param dunsList list of target DUNS, will be standardized
     * @return list of associated guide books that has the same size as the input DUNS list. For each input DUNS,
     * if either the DUNS is not valid or there is no associated guide book, a {@literal null} will be placed
     * at the same index in the returned list.
     */
    List<DunsGuideBook> get(@NotNull String version, @NotNull List<String> dunsList);

    /**
     * Upsert the given list of {@link DunsGuideBook}.
     * @param version target {@link DataCloudVersion#getVersion()}, has to be a valid version
     * @param books list of guide books to update, should not be {@literal null}. Also, {@literal null} items in
     *              the list will be skipped
     */
    void set(@NotNull String version, @NotNull List<DunsGuideBook> books);

    /**
     * Retrieve a {@link DunsGuideBookEntityMgr} instance that is associated with the given datacloud version
     * @param version target {@link DataCloudVersion#getVersion()}, has to be a valid version
     * @return non-null {@link DunsGuideBookEntityMgr} instance
     */
    DunsGuideBookEntityMgr getEntityMgr(@NotNull String version);
}
