package br.com.jesteriruka.jasmin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TableBuilder {

    private Map<String, TableColumn> map = new LinkedHashMap<>();

    public TableColumn id() {
        return bigint("id").primary().increment();
    }

    public TableColumn string(String column) {
        return string(column, 255);
    }

    public TableColumn string(String column, int len) {
        return addColumn(column, "VARCHAR("+len+")");
    }

    public TableColumn uuid(String column) {
        return string(column, 36);
    }

    public TableColumn bool(String column) {
        return tinyint(column);
    }

    public TableColumn tinyint(String column) {
        return addColumn(column, "TINYINT");
    }

    public TableColumn integer(String column) {
        return addColumn(column, "INT");
    }

    public TableColumn decimal(String column) {
        return addColumn(column, "DECIMAL(10,2)");
    }

    public TableColumn decimal(String column, int size, int precision) {
        return addColumn(column, "DECIMAL("+size+","+precision+")");
    }

    public TableColumn bigint(String column) {
        return addColumn(column, "BIGINT");
    }

    public TableColumn text(String column) {
        return addColumn(column, "TEXT");
    }

    public TableColumn mediumtext(String column) {
        return addColumn(column, "MEDIUMTEXT");
    }

    public TableColumn longtext(String column) {
        return addColumn(column, "LONGTEXT");
    }

    public TableColumn date(String column) {
        return addColumn(column, "DATE");
    }

    public TableColumn datetime(String column) {
        return addColumn(column, "DATETIME");
    }

    public TableColumn addColumn(String key, String type) {
        TableColumn col = new TableColumn(type);
        map.put(key.toLowerCase(), col);
        return col;
    }

    public String sql() {
        StringBuilder sb = new StringBuilder("(");
        map.forEach((key, column) -> {
            String type = column.getType();
            if (!column.isNullable() || column.isIncrement())
                type += " NOT NULL";
            if (column.isIncrement())
                type += " AUTO_INCREMENT";
            if (column.isUnique())
                type += " UNIQUE";
            if (column.getDefault() != null)
                type += " DEFAULT ?";
            sb.append('`').append(key).append('`').append(' ').append(type);
            sb.append(',');
        });
        String[] primaries = map.entrySet().stream().filter(e->e.getValue().isPrimary()).map(Map.Entry::getKey)
                .map(s->'`'+s+'`').toArray(String[]::new);
        if (primaries.length == 0) sb.deleteCharAt(sb.length()-1);
        else {
            sb.append("PRIMARY KEY (").append(String.join(",", primaries)).append(')');
        }
        return sb.append(')').toString();
    }

    public Object[] getBindings() {
        return map.values().stream().map(TableColumn::getDefault).filter(Objects::nonNull).toArray();
    }

}
