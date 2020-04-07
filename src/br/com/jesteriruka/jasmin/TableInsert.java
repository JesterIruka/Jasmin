package br.com.jesteriruka.jasmin;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class TableInsert {

    private Table table;
    private String[] columns;

    public TableInsert(Table table, String... columns) {
        this.table = table;
        this.columns = columns;
    }

    public long one(Object... arr) throws SQLException {
        try (PreparedStatement ps = table.getLink().prepareStatement(sql(), Statement.RETURN_GENERATED_KEYS)) {
            for (int x = 0; x < arr.length; x++) {
                ps.setObject(x+1, arr[x]);
            }
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) return rs.getLong(1);
                else return 0;
            }
        }
    }

    public long[] many(List<Object[]> list) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement ps = table.getLink().prepareStatement(sql(), Statement.RETURN_GENERATED_KEYS)) {
            for (Object[] arr : list) {
                for (int x = 0; x < arr.length; x++) {
                    ps.setObject(x+1, arr[x]);
                }
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    while (rs.next()) ids.add(rs.getLong(1));
                }
            }
        }
        return ids.stream().mapToLong(l->l).toArray();
    }

    private String sql() {
        String columns = String.join(",", Stream.of(this.columns).map(s->'`'+s+'`').toArray(String[]::new));
        String marks = String.join(",", Stream.of(this.columns).map(s->"?").toArray(String[]::new));
        return "INSERT INTO `"+table.getName()+"` ("+columns+") VALUES ("+marks+")";
    }
}
