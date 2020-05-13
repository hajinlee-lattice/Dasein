package com.latticeengines.app.repository.reader;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.FileDownload;

public interface FileDownloadReaderRepository extends BaseJpaRepository<FileDownload, Long> {

    FileDownload findByToken(String token);
}
