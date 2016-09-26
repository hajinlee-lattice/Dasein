package com.latticeengines.modelquality.controller;

import java.util.List;

public interface CrudInterface<T> {

    T getByName(String name);

    List<T> getAll();

    String create(T config, Object... params);
}
