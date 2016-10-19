package com.latticeengines.common.exposed.vdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;

public class SpecParser {
    private String spec;
    private ParseTree parseTree;

    public SpecParser(String spec) throws SpecParseException {
        this.spec = spec;
        parse();
    }

    public void setSpec(String spec) throws SpecParseException {
        this.spec = spec;
        parse();
    }

    public ParseTree getParseTree() {
        return parseTree;
    }

    public Map<String, String> getSegmentMap() {
        HashMap<String, String> map = new HashMap<>();
        List<SpecNode> searchResult = BreadthFirstSearchKeyword("LatticeFunctionExpression");
        for (SpecNode node : searchResult) {
            List<SpecNode> children = node.getChildren();
            iterateChildrenAddKeyValuePairs(children, map);
        }
        return map;
    }

    public String getTemplate() {
        StringBuilder sb = new StringBuilder();
        List<SpecNode> searchResult = BreadthFirstSearchKeyword("LatticeFunctionExpression");
        for (SpecNode node : searchResult) {
            List<SpecNode> children = node.getChildren();
            for (int i = 0; i < children.size(); i++) {
                SpecNode child = children.get(i);
                if (child.valueEquals("LatticeFunctionExpressionConstant")) {
                    SpecNode constantNode = child.getChildren().get(0);
                    sb.append(constantNode);
                }
            }
        }
        return sb.toString();
    }

    private void iterateChildrenAddKeyValuePairs(List<SpecNode> children, Map<String, String> map) {
        for (int i = 0; i < children.size(); i++) {
            SpecNode child = children.get(i);
            if (child.valueEquals("LatticeFunctionIdentifier")) {
                SpecNode containerElementName = child.getChildren().get(0);
                SpecNode segmentNode = containerElementName.getChildren().get(0);
                i++;
                child = children.get(i);
                SpecNode guidNode = child.getChildren().get(0);
                map.put(segmentNode.getValueWithoutQuotes(), guidNode.getValueWithoutQuotes());
            } else if (child.valueEquals("LatticeFunctionExpressionConstant")) {
                SpecNode guidNode = child.getChildren().get(0);
                map.put("default", guidNode.getValueWithoutQuotes());
            }
        }
    }

    private List<SpecNode> BreadthFirstSearchKeyword(String keyword) {
        BreadthFirstSearch bfs = new BreadthFirstSearch();
        SpecNodeVisitor visitor = new SpecNodeVisitor(keyword, true);
        bfs.run(parseTree.getRoot(), visitor);
        return visitor.getResult();
    }

    public void parse() throws SpecParseException {
        Stack<String> stack = new Stack<>();
        Stack<SpecNode> nodeStack = new Stack<>();

        spec = trimString(spec);

        String delims = "(),";
        StringTokenizer st = new StringTokenizer(spec, delims, true);

        while (st.hasMoreTokens()) {
            String token = st.nextToken().trim();
            if (isText(token)) {
                SpecNode node = new SpecNode(token);
                nodeStack.push(node);
                stack.push(token);
            } else if (isLeftParenthesis(token)) {
                stack.push(token);
            } else if (isRightParenthesis(token)) {
                LinkedList<SpecNode> children = new LinkedList<SpecNode>();
                while (!isLeftParenthesis(stack.peek())) {
                    stack.pop();
                    children.addFirst(nodeStack.pop());
                }
                if (isLeftParenthesis(stack.peek()))
                    stack.pop();
                nodeStack.peek().setChildren(children);
            }
        }
        ParseTree tree = new ParseTree();
        if (nodeStack.size() == 1) {
            tree.setRoot(nodeStack.pop());
        } else
            throw new SpecParseException("Spec syntax is not correct.");
        parseTree = tree;
    }

    public static String readSpecFromFile(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        return trimString(new String(bytes));
    }

    public static String trimString(String input) {
        return input.replaceAll("(\\r|\\n|\\t|\\s{2,})", "");
    }

    public static boolean isLeftParenthesis(String token) {
        return token.equals("(");
    }

    public static boolean isRightParenthesis(String token) {
        return token.equals(")");
    }

    public static boolean isComma(String token) {
        return token.equals(",");
    }

    public static boolean isText(String token) {
        return !isLeftParenthesis(token) && !isRightParenthesis(token) && !isComma(token);
    }
}
