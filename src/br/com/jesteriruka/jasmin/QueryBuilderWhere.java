package br.com.jesteriruka.jasmin;

public class QueryBuilderWhere {

    private String name;
    private String operator;
    public boolean or;

    QueryBuilderWhere(String name, String operator) {
        this(name, operator, false);
    }

    QueryBuilderWhere(String name, String operator, boolean or) {
        this.name = name;
        this.operator = operator;
        this.or = or;
    }

    public String sql() {
        return "`"+name+"` "+operator+(or?" OR":"");
    }
}
