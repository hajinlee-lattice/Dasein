package com.latticeengines.common.exposed.vdb;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class SpecNodeVisitor implements Visitor {
    private String keyword;
    private boolean exactMatch;
    private boolean ignoreCase;
    private List<SpecNode> result = new ArrayList<>();

    SpecNodeVisitor() {
        this("", true, true);
    }

    SpecNodeVisitor(String keyword) {
        this(keyword, true, true);
    }

    SpecNodeVisitor(String keyword, boolean exactMatch) {
        this(keyword, exactMatch, true);
    }

    private SpecNodeVisitor(String keyword, boolean exactMatch, boolean ignoreCase) {
        this.keyword = keyword;
        this.exactMatch = exactMatch;
        this.ignoreCase = ignoreCase;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public void setExactMatch(boolean exactMatch) {
        this.exactMatch = exactMatch;
    }

    public void setIgnoreCase(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public List<SpecNode> getResult() {
        return result;
    }

    @Override
    public void visit(Object o, VisitorContext ctx) {
        if (o.getClass().equals(SpecNode.class)) {
            SpecNode node = (SpecNode) o;
            if (exactMatch) {
                if (ignoreCase) {
                    addIfEqualKeywordIgnoreCase(node);
                } else {
                    addIfEqualKeyword(node);
                }
            } else {
                if (ignoreCase) {
                    addIfContainKeywordIgnoreCase(node);
                } else {
                    addIfContainKeyword(node);
                }
            }
        }
    }

    private void addIfContainKeywordIgnoreCase(SpecNode node) {
        if (node.containsKeywordIgnoreCase(keyword))
            result.add(node);
    }

    private void addIfContainKeyword(SpecNode node) {
        if (node.containsKeyword(keyword))
            result.add(node);
    }

    private void addIfEqualKeywordIgnoreCase(SpecNode node) {
        if (node.valueEqualsIgnoreCase(keyword))
            result.add(node);
    }

    private void addIfEqualKeyword(SpecNode node) {
        if (node.valueEquals(keyword))
            result.add(node);
    }
}
