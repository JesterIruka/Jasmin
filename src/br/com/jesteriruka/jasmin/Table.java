package br.com.jesteriruka.jasmin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class Table {

    public static Connection defaultLink;

    private String name;
    private Connection link;

    public Table(String name) {
        this(name, defaultLink);
    }

    public Table(String name, Connection link) {
        this.name = name;
        this.link = link;
    }

    public void build(Consumer<TableBuilder> builder) throws SQLException {
        TableBuilder tb = new TableBuilder();
        builder.accept(tb);
        String sql = "CREATE TABLE IF NOT EXISTS `"+name+"` "+tb.sql();
        Object[] binds = tb.getBindings();
        System.out.println(sql);
        try (PreparedStatement ps = link.prepareStatement(sql)) {
            for (int x = 1; x <= binds.length; x++) {
                ps.setObject(x, binds[x-1]);
            }
            ps.executeUpdate();
        }
    }

    public void truncate() throws SQLException {
        try (Statement s = link.createStatement()) {
            s.executeUpdate("TRUNCATE `"+name+"`");
        }
    }

    public TableInsert insert(String... columns) {
        return new TableInsert(this, columns);
    }

    public TableInsert replace(String... columns) {
        return insert(columns).replace();
    }

    public TableSelect select(String... columns) {
        return new TableSelect(this).select(columns);
    }

    public TableSelect where(String key, Object value) {
        return select().where(key, value);
    }

    public TableSelect where(String key, String operator, Object value) {
        return select().where(key, operator, value);
    }

    public TableSelect whereNull(String key) {
        return select().whereNull(key);
    }

    public TableSelect whereNotNull(String key) {
        return select().whereNotNull(key);
    }

    public TableSelect whereMin(String key, Number number) {
        return select().whereMin(key, number);
    }

    public TableSelect whereMax(String key, Number number) {
        return select().whereMax(key, number);
    }

    public TableSelect whereIn(String key, Object[] values) {
        return select().whereIn(key, values);
    }

    public TableSelect whereNotIn(String key, Object[] values) {
        return select().whereNotIn(key, values);
    }

    public TableUpdate update(String... columns) {
        return new TableUpdate(this, columns);
    }

    public TableDelete delete() {
        return new TableDelete(this);
    }

    public String getName() {
        return name;
    }

    public Connection getLink() {
        return link;
    }

}
