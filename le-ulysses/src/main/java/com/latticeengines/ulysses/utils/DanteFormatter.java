package com.latticeengines.ulysses.utils;

import java.util.List;

public interface DanteFormatter<T> {

    String format(T entity);

    List<String> format(List<T> entity);

    class DanteFormat {
    }
}
