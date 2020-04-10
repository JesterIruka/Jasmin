package br.com.jesteriruka.jasmin;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class QueryBuilder<T extends QueryBuilder<T>> {

    private List<QueryBuilderGroup> groups = new ArrayList<>();
    private List<Object> values = new ArrayList<>();

    QueryBuilder() {
        groups.add(new QueryBuilderGroup());
    }

    private QueryBuilderGroup last() {
        return groups.get(groups.size()-1);
    }

    public T where(String field, String operator, Object val) {
        last().add(new QueryBuilderWhere(field, operator+" ?"));
        values.add(val);
        return (T) this;
    }

    public T where(String field, Object val) {
        return where(field, "=", val);
    }

    public T whereIn(String field, Object[] vals) {
        String marks = String.join(",", Stream.of(vals).map(s->"?").toArray(String[]::new));
        last().add(new QueryBuilderWhere(field, "IN ("+marks+")"));
        values.addAll(Arrays.asList(vals));
        return (T) this;
    }

    public T whereNotIn(String field, Object[] vals) {
        String marks = String.join(",", Stream.of(vals).map(s->"?").toArray(String[]::new));
        last().add(new QueryBuilderWhere(field, "NOT IN ("+marks+")"));
        values.addAll(Arrays.asList(vals));
        return (T) this;
    }

    public T whereNull(String field) {
        last().add(new QueryBuilderWhere(field, "IS NULL"));
        return (T) this;
    }

    public T whereNotNull(String field) {
        last().add(new QueryBuilderWhere(field, "IS NOT NULL"));
        return (T) this;
    }

    public T whereBetween(String field, Number min, Number max) {
        return whereMin(field, min).whereMax(field, max);
    }

    public T whereBetween(String field, Date start, Date end) {
        return where(field, ">=", start).where(field, "<=", end);
    }

    public T whereBefore(String field, Date date) {
        return where(field, "<=", date);
    }

    public T whereAfter(String field, Date date) {
        return where(field, ">=", date);
    }

    public T whereMin(String field, Number min) {
        return where(field, ">=", min);
    }

    public T whereMax(String field, Number max) {
        return where(field, "<=", max);
    }

    public T and(Consumer<QueryBuilder<?>> query) {
        groups.add(new QueryBuilderGroup());
        query.accept(this);
        groups.add(new QueryBuilderGroup());
        return (T) this;
    }

    public T or() {
        last().last().or = true;
        return (T) this;
    }

    public T or(Consumer<QueryBuilder<?>> query) {
        groups.add(new QueryBuilderGroup());
        last().or = true;
        query.accept(this);
        groups.add(new QueryBuilderGroup());
        return (T) this;
    }

    protected String sql(boolean where) {
        groups.removeIf(QueryBuilderGroup::empty);
        if (groups.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (QueryBuilderGroup group : groups) {
            if (group.or) {
                sb.append(" OR ").append(group.sql());
            } else if (sb.length() == 0) {
                sb.append(group.sql());
            } else {
                sb.append(" AND ").append(group.sql());
            }
        }
        if (where) return "WHERE "+sb.toString();
        else return sb.toString();
    }

    protected List<Object> getBindings() {
        values.replaceAll(o -> o instanceof UUID ? o.toString() : o);
        return values;
    }
}
