package com.latticeengines.common.exposed.vdb;

public class ParseTree {
    private SpecNode root;

    public ParseTree() {
    }

    public ParseTree(SpecNode root) {
        this.root = root;
    }

    public ParseTree(String value) {
        this.root = new SpecNode(value);
    }

    public SpecNode getRoot() {
        return root;
    }

    public void setRoot(SpecNode root) {
        this.root = root;
    }

    public boolean isRoot(SpecNode node) {
        return node == root;
    }

    public boolean isLeaf(SpecNode node) {
        return node.getChildren().isEmpty();
    }

    public void printParseTree() {
        printParseTree(root, new StringBuilder());
    }

    public void printParseTree(SpecNode node) {
        printNodeValue(node);
        printParseTree(node, new StringBuilder());
    }

    private void printParseTree(SpecNode node, StringBuilder sb) {
        if (node == null)
            return;
        else if (isLeaf(node)) {
            startNewLine();
        } else {
            printTreeNodeRecursively(node, sb);
        }
    }

    private void printTreeNodeRecursively(SpecNode node, StringBuilder sb) {
        sb.append(whiteSpacesByStringLenth(node.getValue()));
        checkAndPrintRootNode(node);
        iterateChildren(node, sb);
    }

    private void iterateChildren(SpecNode node, StringBuilder sb) {
        int childCount = 0;
        boolean lastChild = false;
        for (SpecNode subNode : node.getChildren()) {
            lastChild = printSignByNumOfChildren(node, sb, childCount, lastChild);
            printNodeValue(subNode);
            printParseTree(subNode, createStringBuilderForSubNode(sb, lastChild));
            childCount++;
        }
    }

    private boolean printSignByNumOfChildren(SpecNode node, StringBuilder sb, int childCount, boolean lastChild) {
        if (node.hasOneChild())
            printDirectLink();
        else if (childCount != 0) {
            System.out.print(sb);
            if (childCount != node.numOfChildren() - 1) {
                printBetweenLink();
            } else {
                printLastLink();
                lastChild = true;
            }
        } else
            printFirstLink();
        return lastChild;
    }

    private void checkAndPrintRootNode(SpecNode node) {
        if (isRoot(node)) {
            printNodeValue(node);
        }
    }

    private void printNodeValue(SpecNode node) {
        System.out.print(node.getValue());
    }

    private void startNewLine() {
        System.out.println();
    }

    private void printDirectLink() {
        System.out.print(" ── ");
    }

    private void printBetweenLink() {
        System.out.print(" ├─ ");
    }

    private void printLastLink() {
        System.out.print(" └─ ");
    }

    private void printFirstLink() {
        System.out.print(" ┌─ ");
    }

    private StringBuilder whiteSpacesByStringLenth(String string) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            sb.append(" ");
        }
        return sb;
    }

    private StringBuilder createStringBuilderForSubNode(StringBuilder sb, boolean lastChild) {
        StringBuilder newSb = new StringBuilder(sb);
        adjustStringBuilderForLinkLength(newSb, lastChild);
        return newSb;
    }

    private void adjustStringBuilderForLinkLength(StringBuilder sb, boolean lastChild) {
        if (lastChild)
            sb.append("    ");
        else
            sb.append(" │  ");
    }
}
