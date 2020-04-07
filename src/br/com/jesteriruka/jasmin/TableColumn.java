package br.com.jesteriruka.jasmin;

public class TableColumn {

    private String type;
    private boolean unique,primary,increment,nullable;
    private Object def;

    public TableColumn(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public TableColumn primary() {
        this.primary = true;
        return this;
    }

    public TableColumn increment() {
        this.increment = true;
        return this;
    }

    public TableColumn nullable() {
        this.nullable = true;
        return this;
    }

    public TableColumn unique() {
        this.unique = true;
        return this;
    }

    public TableColumn def(Object def) {
        this.def = def;
        return this;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isIncrement() {
        return increment;
    }

    public boolean isPrimary() {
        return primary;
    }

    public boolean isUnique() {
        return unique;
    }

    public Object getDefault() {
        return def;
    }
}
