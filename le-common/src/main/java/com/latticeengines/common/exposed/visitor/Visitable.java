package com.latticeengines.common.exposed.visitor;

public interface Visitable {

    void accept(Visitor visitor, VisitorContext ctx);
}
