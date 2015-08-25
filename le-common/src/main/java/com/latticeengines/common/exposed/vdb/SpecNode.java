package com.latticeengines.common.exposed.vdb;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class SpecNode implements GraphNode {

    private List<SpecNode> children = new LinkedList<SpecNode>();
    private String value;

    public SpecNode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public String getValueWithoutQuotes() {
        return value.replaceAll("\"", "");
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public List<SpecNode> getChildren() {
        return children;
    }

    public void setChildren(LinkedList<SpecNode> children) {
        this.children = children;
    }

    public SpecNode getChildByKeyword(String keyword) {
        for (SpecNode node : children) {
            if (node.valueEquals(keyword))
                return node;
        }
        return null;
    }

    public SpecNode getChildByIndex(int index) {
        return children.get(index);
    }

    public boolean hasOneChild() {
        return children.size() == 1;
    }

    public int numOfChildren() {
        return children.size();
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    public boolean containsKeywordIgnoreCase(String keyword) {
        keyword = keyword.toLowerCase();
        String lowerCaseValue = value.toLowerCase();
        return lowerCaseValue.contains(keyword);
    }

    public boolean containsKeyword(String keyword) {
        return value.contains(keyword);
    }

    public boolean valueEqualsIgnoreCase(String keyword) {
        keyword = keyword.toLowerCase();
        String lowerCaseValue = value.toLowerCase();
        return lowerCaseValue.equals(keyword);
    }

    public boolean valueEquals(String keyword) {
        return value.equals(keyword);
    }

    @Override
    public String toString() {
        return getValueWithoutQuotes();
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("specnode", children);
        return map;
    }
}
