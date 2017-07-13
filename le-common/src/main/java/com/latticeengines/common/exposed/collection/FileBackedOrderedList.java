package com.latticeengines.common.exposed.collection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBackedOrderedList<T extends Comparable> implements Iterable<T> {

    private static final Logger log = LoggerFactory.getLogger(FileBackedOrderedList.class);
    private static final int NUM_FORKS = 10;

    private final long bufferSize;
    private final String tempDir;
    private final Function<String, T> deserializeFunc;
    private int size = 0;
    private List<T> buffer;
    private Map<String, T> maxiums = new HashMap<>();
    private Comparator<T> comparator = new NullFirstComparator<T>();

    public static FileBackedOrderedList<Integer> newIntList(long bufferSize) {
        return new FileBackedOrderedList<>(bufferSize, s -> "".equals(s) ? null : Integer.valueOf(s));
    }

    public static FileBackedOrderedList<Long> newLongList(long bufferSize) {
        return new FileBackedOrderedList<>(bufferSize, s -> "".equals(s) ? null : Long.valueOf(s));
    }

    public static FileBackedOrderedList<Float> newFloatList(long bufferSize) {
        return new FileBackedOrderedList<>(bufferSize, s -> "".equals(s) ? null : Float.valueOf(s));
    }

    public static FileBackedOrderedList<Double> newDoubleList(long bufferSize) {
        return new FileBackedOrderedList<>(bufferSize, s -> "".equals(s) ? null : Double.valueOf(s));
    }

    public static FileBackedOrderedList<String> newStrList(long bufferSize) {
        return new FileBackedOrderedList<>(bufferSize, s -> "".equals(s) ? null : s);
    }
    
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
        log.debug("Dumping a buffer of size " + buffer.size());
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
        log.debug(segments.size() + " segments to be inserted.");
        for (Map.Entry<String, List<T>> segment : segments.entrySet()) {
            dumpListToFile(segment.getValue(), segment.getKey());
        }
        buffer.clear();
    }

    @SuppressWarnings("unchecked")
    private void dumpListToFile(List<T> sortedList, String fileName) {
        if (sortedList.isEmpty()) {
            return;
        }
        File file = new File(tempDir + File.separator + fileName);
        if (!file.exists()) {
            sortedList.sort(comparator);
            T max = sortedList.get(sortedList.size() - 1);
            try {
                FileUtils.writeLines(file, sortedList);
            } catch (IOException e) {
                throw new RuntimeException("Failed to dump buffer to local file " + file, e);
            }
            maxiums.put(fileName, max);
        } else {
            insertAndSplit(sortedList, fileName);
        }
    }

    @SuppressWarnings("unchecked")
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
        lines.forEach(l -> sortedList.add(deserializeFunc.apply(l)));
        sortedList.sort(comparator);
        if (sortedList.size() > bufferSize) {
            int chunkSize = Math.max(sortedList.size() / NUM_FORKS, 1);
            Iterator<T> iterator = sortedList.listIterator();
            List<String> descriptions = new ArrayList<>();
            for (int i = 0; i < NUM_FORKS; i++) {
                List<T> chunk = new ArrayList<>();
                if (i == NUM_FORKS - 1) {
                    while (iterator.hasNext()) {
                        chunk.add(iterator.next());
                    }
                } else {
                    for (int j = 0; j < chunkSize; j++) {
                        if (iterator.hasNext()) {
                            chunk.add(iterator.next());
                        }
                    }
                }
                String child = parent + String.valueOf(i);
                descriptions.add(String.format("%s [%d]", child, chunk.size()));
                dumpListToFile(chunk, child);
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Split %s into %s", parent, StringUtils.join(descriptions, " ")));
            }
        } else {
            log.debug("Extend " + parent + " to " + sortedList.size() + " items.");
            dumpListToFile(sortedList, parent);
        }
    }

    @SuppressWarnings("unchecked")
    private String findInsertingFile(T item, List<String> fileNames) {
        for (String fileName : fileNames) {
            T max = maxiums.get(fileName);
            if (comparator.compare(max, item) >= 0) {
                return fileName;
            }
        }
        return fileNames.isEmpty() ? "0" : fileNames.get(fileNames.size() - 1);
    }

    @SuppressWarnings("unchecked")
    public Iterator<T> iterator() {
        if (maxiums.isEmpty()) {
            buffer.sort(comparator);
            return buffer.listIterator();
        } else {
            dumpBuffer();
            return new FileBackedIterator();
        }
    }

    private void shutdownHook() {
        FileUtils.deleteQuietly(new File(tempDir));
    }

    @SuppressWarnings("unchecked")
    private static class NullFirstComparator<T> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return -1;
            }
            if (o2 == null) {
                return 1;
            }
            return ((Comparable<T>) o1).compareTo(o2);
        }
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
