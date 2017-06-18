//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.querydsl.sql;

import com.google.common.collect.Lists;
import com.querydsl.core.types.ConstantImpl;
import com.querydsl.core.types.Expression;
import com.querydsl.sql.WindowFunction;
import java.util.List;

public class WindowRows<A> {
    private static final String AND = " and";
    private static final String BETWEEN = " between";
    private static final String CURRENT_ROW = " current row";
    private static final String FOLLOWING = " following";
    private static final String PRECEDING = " preceding";
    private static final String UNBOUNDED = " unbounded";
    private final WindowFunction<A> rv;
    private final StringBuilder str = new StringBuilder();
    private final List<Expression<?>> args = Lists.newArrayList();
    private int offset;

    public WindowRows(WindowFunction<A> windowFunction, String prefix, int offset) {
        this.rv = windowFunction;
        this.offset = offset;
        this.str.append(prefix);
    }

    @SuppressWarnings("unchecked")
    public WindowRows<A>.Between between() {
        this.str.append(" between");
        return new WindowRows.Between();
    }

    public WindowFunction<A> unboundedPreceding() {
        this.str.append(" unbounded");
        this.str.append(" preceding");
        return this.rv.withRowsOrRange(this.str.toString(), this.args);
    }

    public WindowFunction<A> currentRow() {
        this.str.append(" current row");
        return this.rv.withRowsOrRange(this.str.toString(), this.args);
    }

    public WindowFunction<A> preceding(Expression<Integer> expr) {
        this.args.add(expr);
        this.str.append(" preceding");
        this.str.append(" {" + this.offset++ + "}");
        return this.rv.withRowsOrRange(this.str.toString(), this.args);
    }

    public WindowFunction<A> preceding(int i) {
        return this.preceding(ConstantImpl.create(i));
    }

    public class BetweenAnd {
        public BetweenAnd() {
            WindowRows.this.str.append(" and");
        }

        public WindowFunction<A> unboundedFollowing() {
            WindowRows.this.str.append(" unbounded");
            WindowRows.this.str.append(" following");
            return WindowRows.this.rv.withRowsOrRange(WindowRows.this.str.toString(), WindowRows.this.args);
        }

        public WindowFunction<A> currentRow() {
            WindowRows.this.str.append(" current row");
            return WindowRows.this.rv.withRowsOrRange(WindowRows.this.str.toString(), WindowRows.this.args);
        }

        public WindowFunction<A> preceding(Expression<Integer> expr) {
            WindowRows.this.args.add(expr);
            WindowRows.this.str.append(" preceding");
            WindowRows.this.str.append(" {" + WindowRows.this.offset++ + "}");
            return WindowRows.this.rv.withRowsOrRange(WindowRows.this.str.toString(), WindowRows.this.args);
        }

        public WindowFunction<A> preceding(int i) {
            return this.preceding(ConstantImpl.create(i));
        }

        public WindowFunction<A> following(Expression<Integer> expr) {
            WindowRows.this.args.add(expr);
            WindowRows.this.str.append(" {" + WindowRows.this.offset++ + "}");
            WindowRows.this.str.append(" following");
            return WindowRows.this.rv.withRowsOrRange(WindowRows.this.str.toString(), WindowRows.this.args);
        }

        public WindowFunction<A> following(int i) {
            return this.following(ConstantImpl.create(i));
        }
    }

    public class Between {
        public Between() {
        }

        public WindowRows<A>.BetweenAnd unboundedPreceding() {
            WindowRows.this.str.append(" unbounded");
            WindowRows.this.str.append(" preceding");
            return WindowRows.this.new BetweenAnd();
        }

        public WindowRows<A>.BetweenAnd currentRow() {
            WindowRows.this.str.append(" current row");
            return WindowRows.this.new BetweenAnd();
        }

        public WindowRows<A>.BetweenAnd preceding(Expression<Integer> expr) {
            WindowRows.this.args.add(expr);
            WindowRows.this.str.append(" preceding");
            WindowRows.this.str.append(" {" + WindowRows.this.offset++ + "}");
            return WindowRows.this.new BetweenAnd();
        }

        public WindowRows<A>.BetweenAnd preceding(int i) {
            return this.preceding(ConstantImpl.create(i));
        }

        public WindowRows<A>.BetweenAnd following(Expression<Integer> expr) {
            WindowRows.this.args.add(expr);
            WindowRows.this.str.append(" following");
            WindowRows.this.str.append(" {" + WindowRows.this.offset++ + "}");
            return WindowRows.this.new BetweenAnd();
        }

        public WindowRows<A>.BetweenAnd following(int i) {
            return this.following(ConstantImpl.create(i));
        }
    }
}
