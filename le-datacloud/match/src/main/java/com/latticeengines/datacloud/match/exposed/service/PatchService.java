package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLog;
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
     * @param patchLogs list of logs to upload
     * @return file path that contains the uploaded logs
     */
    String uploadPatchLog(@NotNull List<PatchLog> patchLogs);
}
