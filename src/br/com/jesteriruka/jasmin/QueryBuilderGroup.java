package br.com.jesteriruka.jasmin;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilderGroup {

    public List<QueryBuilderWhere> wheres = new ArrayList<>();
    public boolean or;

    public QueryBuilderWhere last() {
        return wheres.get(wheres.size()-1);
    }

    public void add(QueryBuilderWhere where) {
        wheres.add(where);
    }

    public boolean empty() {
        return wheres.isEmpty();
    }

    public String sql() {
        StringBuilder sb = new StringBuilder();
        for (QueryBuilderWhere w : wheres) {
            if (sb.toString().endsWith("OR")) {
                sb.append(" ").append(w.sql());
            } else if (sb.length() == 0) {
                sb.append(w.sql());
            } else {
                sb.append(" AND ").append(w.sql());
            }
        }
        return "(?)".replace("?", sb.toString());
    }
}
