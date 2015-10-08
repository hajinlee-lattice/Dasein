package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.InListOperatorNode;
import com.foundationdb.sql.parser.RowConstructorNode;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.parser.ValueNodeList;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;

public class ExpressionParserVisitorBase implements Visitor {

    private Map<String, Object> expressions = new HashMap<>();

    public Map<String, Object> getExpressions() {
        return expressions;
    }

    @Override
    public Visitable visit(Visitable parseTreeNode) throws StandardException {
        if (parseTreeNode instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode op = (BinaryRelationalOperatorNode) parseTreeNode;

            ValueNode left = op.getLeftOperand();
            ValueNode right = op.getRightOperand();
            String colName = null;
            if (left instanceof ColumnReference) {
                colName = ((ColumnReference) left).getColumnName();
            } else {
                throw new UnsupportedOperationException(
                        "All binary expressions must have a column reference as its left hand side.");
            }

            if (right instanceof ConstantNode) {
                expressions.put(colName, ((ConstantNode) right).getValue());
            } else {
                throw new UnsupportedOperationException(
                        "Non-constants on the right hand side of a binary operator is unsupported.");
            }
        }

        if (parseTreeNode instanceof InListOperatorNode) {
            InListOperatorNode op = (InListOperatorNode) parseTreeNode;
            RowConstructorNode left = op.getLeftOperand();
            ValueNode column = left.getNodeList().get(0);
            String colName = null;
            if (column instanceof ColumnReference) {
                colName = ((ColumnReference) column).getColumnName();
            } else {
                throw new UnsupportedOperationException(
                        "IN expressions must have a column reference as its left hand side.");
            }
            RowConstructorNode right = op.getRightOperandList();
            ValueNodeList valueList = right.getNodeList();
            List<Object> values = new ArrayList<>();
            for (ValueNode value : valueList) {
                if (!(value instanceof ConstantNode)) {
                    throw new UnsupportedOperationException(
                            "Non-constants on the right hand side of an IN operator is unsupported.");
                }
                values.add(((ConstantNode) value).getValue());
            }
            expressions.put(colName, values);
        }
        return null;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

}
