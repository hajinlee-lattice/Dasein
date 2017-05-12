package com.latticeengines.common.exposed.collection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.Collections;

public class FileBackedOrderedList<T> implements Iterable<T> {

    private static final Log log = LogFactory.getLog(FileBackedOrderedList.class);

    private final long bufferSize;
    private final String tempDir;
    private final Function<String, T> deserializeFunc;
    private int size = 0;
    private List<T> buffer;
    private Map<String, T> maxiums = new HashMap<>();
    private int rootFileId = 0;

    public FileBackedOrderedList(long bufferSize, Function<String, T> deserializeFunc) {
        this.bufferSize = bufferSize;
        this.deserializeFunc = deserializeFunc;
        this.tempDir = UUID.randomUUID().toString();
        buffer = new ArrayList<>();
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownHook));
    }

    @SuppressWarnings("unchecked")
    public void add(Object item) {
        buffer.add((T) item);
        if (buffer.size() >= bufferSize) {
            dumpBuffer();
        }
        size++;
    }

    public int size() {
        return size;
    }

    private void dumpBuffer() {
        if (buffer.isEmpty()) {
            return;
        }

        log.debug("Dumping a buffer of " + buffer.size() + " items.");
        Map<String, List<T>> segments = new HashMap<>();
        List<String> fileNames = new ArrayList<>(maxiums.keySet());
        Collections.sort(fileNames);
        for (T item : buffer) {
            String insertingFile = findInsertingFile(item, fileNames);
            if (!segments.containsKey(insertingFile)) {
                segments.put(insertingFile, new ArrayList<>());
            }
            segments.get(insertingFile).add(item);
        }
        for (Map.Entry<String, List<T>> segment : segments.entrySet()) {
            dumpListToFile(segment.getValue(), segment.getKey());
        }
        buffer.clear();
    }

    private void dumpListToFile(List<T> sortedList, String fileName) {
        if (StringUtils.isBlank(fileName)) {
            fileName = String.valueOf(rootFileId++);
        }
        File file = new File(tempDir + File.separator + fileName);
        if (!file.exists()) {
            Collections.sort(sortedList);
            T max = sortedList.get(sortedList.size() - 1);
            try {
                FileUtils.writeLines(file, sortedList);
            } catch (IOException e) {
                throw new RuntimeException("Failed to dump buffer to local file " + file, e);
            }
            maxiums.put(fileName, max);
            log.debug("Dumping " + sortedList.size() + " items with maximum " + max + " to file "
                    + fileName);
        } else {
            insertAndSplit(sortedList, fileName);
        }
    }

    private void insertAndSplit(List<T> list, String parent) {
        File parentFile = new File(tempDir + File.separator + parent);
        List<String> lines;
        try {
            lines = FileUtils.readLines(parentFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read lines for the parent file " + parent);
        }
        maxiums.remove(parent);
        FileUtils.deleteQuietly(parentFile);
        List<T> sortedList = new ArrayList<>(list);
        for (String line : lines) {
            sortedList.add(deserializeFunc.apply(line));
        }
        Collections.sort(sortedList);
        if (sortedList.size() > bufferSize) {
            int halfSize = Math.max(sortedList.size() / 2, 1);
            Iterator<T> iterator = sortedList.listIterator();
            List<T> firstHalf = new ArrayList<>();
            for (int i = 0; i < halfSize; i++) {
                if (iterator.hasNext()) {
                    firstHalf.add(iterator.next());
                }
            }
            List<T> secondHalf = new ArrayList<>();
            while (iterator.hasNext()) {
                secondHalf.add(iterator.next());
            }
            dumpListToFile(firstHalf, parent + "0");
            dumpListToFile(secondHalf, parent + "1");
            if (log.isDebugEnabled()) {
                log.debug(String.format("Split %s into %d items in %s and %d items in %s", parent, firstHalf.size(),
                        parent + "0", secondHalf.size(), parent + "1"));
            }
        } else {
            log.debug("Extend " + parent + " to " + sortedList.size() + " items.");
            dumpListToFile(sortedList, parent);
        }
    }

    @SuppressWarnings("unchecked")
    private String findInsertingFile(T item, List<String> fileNames) {
        for (String fileName : fileNames) {
            Comparable<T> max = (Comparable<T>) maxiums.get(fileName);
            if (max.compareTo(item) >= 0) {
                return fileName;
            }
        }
        // means a new file
        return "";
    }

    public Iterator<T> iterator() {
        dumpBuffer();
        return new FileBackedIterator();
    }

    private void shutdownHook() {
        FileUtils.deleteQuietly(new File(tempDir));
    }

    public class FileBackedIterator implements Iterator<T> {

        Iterator<String> lineIterator;
        Iterator<T> bufferIterator;
        Iterator<String> fileIterator;

        FileBackedIterator() {
            List<String> fileNames = new ArrayList<>(maxiums.keySet());
            Collections.sort(fileNames);
            fileIterator = fileNames.listIterator();
            if (fileIterator.hasNext()) {
                String currentFile = fileIterator.next();
                lineIterator = getLineIterator(currentFile);
            }
            bufferIterator = buffer.listIterator();
        }

        public T next() {
            if (lineIterator != null && lineIterator.hasNext()) {
                String line = lineIterator.next();
                return deserializeFunc.apply(line);
            }
            if (bufferIterator.hasNext()) {
                return bufferIterator.next();
            }
            throw new RuntimeException("No next item");
        }

        @Override
        public boolean hasNext() {
            if (lineIterator != null && lineIterator.hasNext()) {
                return true;
            }
            while ((lineIterator != null && !lineIterator.hasNext()) && fileIterator.hasNext()) {
                String currentFile = fileIterator.next();
                lineIterator = getLineIterator(currentFile);
                if (lineIterator.hasNext()) {
                    return true;
                }
            }
            return bufferIterator.hasNext();
        }

        private Iterator<String> getLineIterator(String file) {
            try {
                List<String> lines = FileUtils.readLines(new File(tempDir + File.separator + file));
                return lines.listIterator();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read lines from local file", e);
            }
        }
    }

}
