package com.emini.presto.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.facebook.presto.jdbc.PrestoResultSet;


public abstract class PrestoResultSetBinder {

    private PrestoResultSet prestoResultSet;

    public PrestoResultSetBinder() {}

    public PrestoResultSetBinder(PrestoResultSet prestoResultSet) {
        this.prestoResultSet = prestoResultSet;
    }

    public PrestoResultSet getPrestoResultSet() {
        return prestoResultSet;
    }

    public void setPrestoResultSet(PrestoResultSet prs) {
        this.prestoResultSet = prs;
    }

    public abstract void bind(ResultSet rs) throws SQLException;
}

