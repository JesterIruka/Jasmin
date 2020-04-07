package br.com.jesteriruka.jasmin;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class TableDelete extends QueryBuilder<TableDelete> {

    private Table table;

    public TableDelete(Table table) {
        this.table = table;
    }

    public void exec() throws SQLException {
        make();
    }

    public void make() throws SQLException {
        String sql = "DELETE FROM `"+table.getName()+"` "+sql(true);
        try (PreparedStatement ps = table.getLink().prepareStatement(sql)) {
            for (int x = 1 ; x <= getBindings().size(); x++) {
                ps.setObject(x, getBindings().get(x-1));
            }
            ps.executeUpdate();
        }
    }
}
