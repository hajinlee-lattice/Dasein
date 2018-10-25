package com.latticeengines.datacloud.match.exposed.service;

import java.util.Date;
import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLog;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLogFile;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;

public interface PatchService {

    LookupUpdateResponse patch(List<LookupUpdateRequest> updateRequests);

    /**
     * Patch all input {@link PatchBook} entries and generate one {@link PatchLog} for each entry. In dry run mode,
     * the same patch logs will be returned (as normal mode), however, no data will actually be patched
     *
     * @param books list of {@link PatchBook} entries to be patched, should not be {@literal null}
     * @param dataCloudVersion target DataCloud version to patch, should not be {@literal null}
     * @param mode patch mode, should not be {@literal null}
     * @param dryRun flag to indicate whether this is in dry run mode
     * @return list of patch logs to describe patch result
     */
    List<PatchLog> lookupPatch(
            @NotNull List<PatchBook> books, @NotNull String dataCloudVersion, @NotNull PatchMode mode, boolean dryRun);

    /**
     * Upload given list of {@link PatchLog} and return the log file path
     *
     * @param mode patch mode
     * @param type patch book type
     * @param dryRun dry run flag
     * @param dataCloudVersion DataCloudVersion used for patching
     * @param startAt start time of the patching operation
     * @param patchLogs list of logs to upload
     * @return log file instance that contains related information
     */
    PatchLogFile uploadPatchLog(
            @NotNull PatchMode mode, @NotNull PatchBook.Type type, boolean dryRun,
            @NotNull String dataCloudVersion, @NotNull Date startAt, @NotNull List<PatchLog> patchLogs);
}
