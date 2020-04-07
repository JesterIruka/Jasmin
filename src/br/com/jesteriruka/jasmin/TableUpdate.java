package br.com.jesteriruka.jasmin;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

public class TableUpdate extends QueryBuilder<TableUpdate> {

    private Table table;
    private String[] columns;
    private Object[] values;

    public TableUpdate(Table table, String... columns) {
        this.table = table;
        this.columns = columns;
    }

    public TableUpdate set(Object... values) {
        this.values = values;
        return this;
    }

    public void exec() throws SQLException {
        make();
    }

    public void make() throws SQLException {
        QueryBuilder<?> builder = new QueryBuilder<>();
        for (int i = 0; i < columns.length; i++) {
            builder.where(columns[i], values[i]);
        }

        List<Object> a = builder.getBindings();
        List<Object> b = getBindings();

        String sql = "UPDATE `"+table.getName()+"` SET "+builder.sql(false)+" "+sql(true);
        try (PreparedStatement ps = table.getLink().prepareStatement(sql)) {
            int i = 1;
            for (Object o : a) {
                ps.setObject(i++, o);
            }
            for (Object o : b) {
                ps.setObject(i++, o);
            }
            ps.executeUpdate();
        }
    }
}
