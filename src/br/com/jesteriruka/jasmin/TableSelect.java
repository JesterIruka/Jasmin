package br.com.jesteriruka.jasmin;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableSelect extends QueryBuilder<TableSelect> {

    private Table table;
    private String columns = "*";
    private Integer limit,offset;
    private String order;

    public TableSelect(Table table) {
        this.table = table;
    }

    public TableSelect select(String... columns) {
        if (columns.length == 0) this.columns = "*";
        else this.columns = String.join(",", Stream.of(columns).map(s->"`"+s+"`").toArray(String[]::new));
        return this;
    }

    public TableSelect limit(int len) {
        limit = len;
        return this;
    }

    public TableSelect offset(int len) {
        offset = len;
        return this;
    }

    public TableSelect desc(String key) {
        order = "ORDER BY `"+key+"` DESC";
        return this;
    }

    public TableSelect asc(String key) {
        order = "ORDER BY `"+key+"` ASC";
        return this;
    }

    public Jasmin first() throws SQLException {
        return limit(1).get().stream().findFirst().orElse(Jasmin.EMPTY);
    }

    public <T> T first(Function<Jasmin, T> function) throws SQLException {
        return function.apply(first());
    }

    public List<Object> pluck(String column) throws SQLException {
        return select(column).get().stream().map(j->j.get(column)).collect(Collectors.toList());
    }

    public List<Long> pluckId() throws SQLException {
        return select("id").get().stream().map(Jasmin::id).collect(Collectors.toList());
    }

    public List<String> pluckString(String column) throws SQLException {
        return select(column).get().stream().map(j->j.getString(column)).collect(Collectors.toList());
    }

    public List<Long> pluckLong(String column) throws SQLException {
        return select(column).get().stream().map(j->j.getLong(column)).collect(Collectors.toList());
    }

    public List<Boolean> pluckBoolean(String column) throws SQLException {
        return select(column).get().stream().map(j->j.getBoolean(column)).collect(Collectors.toList());
    }

    public List<Long> ids() throws SQLException {
        return select("id").get().stream().map(Jasmin::id).collect(Collectors.toList());
    }

    public <K,V> Map<K, V> map(String key, String val, Class<? extends K> KEY, Class<? extends V> VAL) throws SQLException {
        Map<K, V> map = new HashMap<>();
        List<Jasmin> list = select(key, val).get();
        list.forEach(j -> {
            map.put(j.get(key, KEY), j.get(val, VAL));
        });
        return map;
    }

    public List<Jasmin> get() throws SQLException {
        String sql = ("SELECT "+columns+" FROM `"+table.getName()+"` "+sql(true)).trim();
        if (limit != null) sql+= " LIMIT "+limit;
        if (offset != null) sql+= " OFFSET "+offset;
        if (order != null) sql+= " "+order;

        try (PreparedStatement ps = table.getLink().prepareStatement(sql)) {
            List<Object> binds = getBindings();
            for (int x = 1; x <= binds.size(); x++) ps.setObject(x, binds.get(x-1));
            try (ResultSet rs = ps.executeQuery()) {
                return Jasmin.readAll(rs);
            }
        }
    }

    public <T> List<T> get(Function<Jasmin, T> function) throws SQLException {
        return get().stream().map(function).collect(Collectors.toList());
    }

}
