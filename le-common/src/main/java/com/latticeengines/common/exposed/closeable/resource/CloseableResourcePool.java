package com.latticeengines.common.exposed.closeable.resource;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CloseableResourcePool {

    private List<Closeable> closeableList = new ArrayList<>();

    public List<Closeable> getCloseables() {
        return closeableList;
    }

    public void setCloseables(List<Closeable> closableList) {
        this.closeableList = closableList;
    }

    public void addCloseable(Closeable closable) {
        this.closeableList.add(closable);
    }

    public void close() throws IOException {
        for(Closeable closeable : closeableList){
            closeable.close();
        }
    }
}
