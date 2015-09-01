package com.latticeengines.admin.dynamicopts.impl;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.admin.dynamicopts.OptionsProvider;

public class SubdirectoryOptionsProvider implements OptionsProvider {

    private static final Log log = LogFactory.getLog(SubdirectoryOptionsProvider.class);
    private final boolean recursive;
    private final List<String> options;
    private WatchService watcher;
    private final Map<WatchKey, Path> keys = new HashMap<>();
    private boolean initialized = false;
    private Thread watcherThread;

    protected final Path path;

    public SubdirectoryOptionsProvider(Path path, boolean recursive) {
        this.recursive = recursive;
        this.path = path;
        File dir = this.path.toFile();
        if (dir.isFile()) {
            FileUtils.deleteQuietly(dir);
        }
        if (!dir.exists()) {
            try {
                FileUtils.forceMkdir(path.toFile());
            } catch (IOException e) {
                // ignore
            }
        }
        this.options = readSubdirectories();
        // startWatcherThread();
    }

    public SubdirectoryOptionsProvider(Path path) {
        this(path, false);
    }

    public SubdirectoryOptionsProvider(String path) {
        this(FileSystems.getDefault().getPath(path));
    }

    private List<String> readSubdirectories() {
        List<String> options = new ArrayList<>();
        File dir = this.path.toFile();
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory())
                        options.add(file.getName());
                }
            }
        }
        return options;
    }

    private void updateOptions() {
        options.clear();
        options.addAll(readSubdirectories());
    }

    @Override
    public List<String> getOptions() {
        // if (!watcherIsWorking()) {
        // directly read option list
        updateOptions();
        // startWatcherThread();
        // }
        return options;
    }

    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        if (initialized) {
            Path prev = keys.get(key);
            if (prev != null && !dir.equals(prev)) {
                log.info(String.format("Update directory WatchKey: %s -> %s", prev, dir));
            }
        }
        keys.put(key, dir);
    }

    private void registerAll(final Path start) throws IOException {
        log.info(String.format("Scanning %s to setup the watcher ...", start));
        this.initialized = false;

        if (this.recursive) {
            Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    register(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            register(start);
        }

        this.initialized = true;
        log.info(String.format("Scanning %s done.", start));
    }

    private void startWatching() throws IOException, InterruptedException {
        log.info(String.format("Start watching path %s as an options provider.", path));
        while (true) {
            // wait for key to be signalled
            WatchKey key;
            key = watcher.take();
            if (key != null) {
                Path dir = keys.get(key);
                if (dir == null)
                    continue;

                for (WatchEvent<?> event : key.pollEvents()) {
                    // Context for directory entry event is the file name of
                    // entry
                    WatchEvent<Path> watchEvent = cast(event);
                    Kind<?> kind = event.kind();

                    Path child = dir.resolve(watchEvent.context());
                    Path relativePath = path.relativize(child);

                    // refresh option list
                    log.info(String.format("%s: %s", event.kind().name(), relativePath));
                    updateOptions();

                    // if directory is created, then register it and its
                    // sub-directories
                    if (kind == ENTRY_CREATE && recursive && Files.isDirectory(child, NOFOLLOW_LINKS)) {
                        registerAll(child);
                    }

                }

                // reset key and remove from set if directory no longer
                // accessible
                boolean valid = key.reset();
                if (!valid) {
                    keys.remove(key);
                    // all directories are inaccessible
                    if (keys.isEmpty()) {
                        log.error(String.format("The options provider path %s becomes unwatchable.", path));
                        break;
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    @SuppressWarnings("unused")
    private void startWatcherThread() {
        if (watcherThread != null) {
            watcherThread.interrupt();
        }
        watcherThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    watcher = FileSystems.getDefault().newWatchService();
                    registerAll(path);
                    startWatching();
                    watcher.close();
                } catch (IOException | InterruptedException e) {
                    // ignore
                }
            }
        });
        watcherThread.start();
    }

    @SuppressWarnings("unused")
    private boolean watcherIsWorking() {
        return watcherThread != null && watcherThread.isAlive();
    }
}
