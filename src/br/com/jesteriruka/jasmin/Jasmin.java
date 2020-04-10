package br.com.jesteriruka.jasmin;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

public class Jasmin {

    public static Jasmin EMPTY = new Jasmin();

    public static Jasmin read(ResultSet rs) throws SQLException {
        return new Jasmin(rs);
    }

    public static List<Jasmin> readAll(ResultSet rs) throws SQLException {
        List<Jasmin> list = new ArrayList<>();
        while (rs.next()) list.add(read(rs));
        return list;
    }

    private Map<String, Object> columns = new HashMap<>();

    private Jasmin() {}

    public Jasmin(ResultSet rs) throws SQLException {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            columns.put(rs.getMetaData().getColumnName(i).toLowerCase(), rs.getObject(i));
        }
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public <T> T map(Function<Jasmin, T> function) {
        return function.apply(this);
    }

    public boolean has(String column) {
        return columns.containsKey(column.toLowerCase());
    }

    public Object get(String column) {
        return columns.get(column.toLowerCase());
    }

    public Number getNumber(String column) {
        return opt(column, Number.class).orElse(0);
    }

    public String getString(String column) {
        return opt(column).map(Object::toString).orElse(null);
    }

    public UUID getUniqueId(String column) {
        return opt(column).map(o->UUID.fromString(o.toString())).orElse(null);
    }

    public <T extends Enum<T>> T getEnum(String column, Class<T> clazz) {
        String raw = getString(column);
        if (raw == null) return null;
        try {
            return Enum.valueOf(clazz, raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public Date getDate(String column) {
        return opt(column, Date.class).orElse(null);
    }

    private Optional<Object> opt(String key) {
        return Optional.ofNullable(get(key));
    }

    private <T> Optional<T> opt(String key, Class<? extends T> clazz) {
        Object o = get(key);
        if (o != null && clazz.isAssignableFrom(o.getClass())) return Optional.of((T)o);
        return Optional.empty();
    }

    public long getLong(String column) {
        return getNumber(column).longValue();
    }

    public double getDouble(String column) {
        return getNumber(column).doubleValue();
    }

    public float getFloat(String column) {
        return getNumber(column).floatValue();
    }

    public int getInt(String column) {
        return getNumber(column).intValue();
    }

    public byte getByte(String column) {
        return getNumber(column).byteValue();
    }

    public byte[] getBase64(String column) {
        return opt(column).map(o->Base64.getDecoder().decode(o.toString())).orElse(null);
    }

    public boolean getBoolean(String column) {
        Object o = get(column);
        if (o instanceof Boolean) return (Boolean) o;
        else if (o instanceof Number) return ((Number) o).byteValue()==1;
        return false;
    }

    public long id() {
        return getLong("id");
    }

    public <T> T get(String col, Class<? extends T> TYPE) {
        Object o = get(col);
        if (o == null) return null;
        else if (TYPE.isAssignableFrom(o.getClass())) return (T) o;
        return null;
    }

    @Override
    public String toString() {
        return columns.toString();
    }
}
