package com.latticeengines.app.exposed.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.latticeengines.app.exposed.service.FileDownloader;
import com.latticeengines.domain.exposed.pls.FileDownloadConfig;

public final class FileDownloaderRegistry {

    private static ConcurrentMap<String, FileDownloader<?>> map = new ConcurrentHashMap<>();

    protected FileDownloaderRegistry() {
        throw new UnsupportedOperationException();
    }

    public static void register(FileDownloader<?> downloader) {
        map.put(downloader.configClz().getCanonicalName(), downloader);
    }

    @SuppressWarnings("unchecked")
    public static <T extends FileDownloadConfig> FileDownloader<T> getDownloader(Class<T> clz) {
        return (FileDownloader<T>) map.get(clz.getCanonicalName());
    }

}
