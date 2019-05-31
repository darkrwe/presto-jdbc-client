package com.emini.presto.jdbc;

import com.emini.presto.config.JDBCConfig;
import com.emini.presto.resultset.PrestoResultSetBinder;
import com.facebook.presto.jdbc.PrestoResultSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class PrestoJDBCClient {

    private static final String PRESTO_DRIVER_NAME = "com.facebook.presto.jdbc.PrestoDriver";
    private final JDBCConfig config;

    public JDBCConfig getConfig() {
        return config;
    }

    public PrestoJDBCClient(JDBCConfig config) {
        this.config = config;
        try {
            init();
        } catch (ClassNotFoundException ex) {
        }
    }

    private void init() throws ClassNotFoundException {
        Class.forName(PRESTO_DRIVER_NAME);
    }

    public void executeQuery(final String schema, final String user,
                             final String sql, PrestoResultSetBinder rsb)
            throws SQLException {
        if (schema != null) {
            setSchema(schema);
        } else {
            setSchema("default");
        }
        getConfig().setUser(user);
        Connection con = getConnection();
        Statement statement = con.createStatement();
        execute(statement, sql, rsb, con);
    }

    public void executeCancelableQuery(Statement statement, Connection con,
                                       final String schema, final String user,
                                       final String sql, PrestoResultSetBinder rsb)
            throws SQLException {
        if (schema != null) {
            setSchema(schema);
        } else {
            setSchema("default");
        }
        getConfig().setUser(user);
        execute(statement, sql, rsb, con);
    }

    public Object executeSimpleQuery(final String schema, final String user, final String sql)
            throws SQLException {
        getConfig().setUser(user);
        Connection con = getConnection();
        Statement statement = con.createStatement();
        try {
            ResultSet rs = statement.executeQuery(sql);
            Object obj = null;
            if (rs.next()) {
                obj = rs.getObject(1);
            }
            return obj;
        } finally {
            close(statement, con);
        }
    }

    public void setSchema(String schema) {
        String connectionUrl = config.getConnectionURL();
        StringTokenizer st = new StringTokenizer(connectionUrl, "/");
        ArrayList<String> tokenizedUrl = new ArrayList();
        while (st.hasMoreTokens()) {
            tokenizedUrl.add(st.nextToken());
        }
        connectionUrl = tokenizedUrl.get(0) + "//" + tokenizedUrl.get(1) + "/" + tokenizedUrl.get(2) + "/" + schema;
        config.setConnectionURL(connectionUrl);
    }

    private void close(Statement s, Connection con) throws SQLException {
        s.close();
        con.close();
    }

    public Connection getConnection() throws SQLException {
        Connection con = DriverManager.getConnection(config.getConnectionURL(), config.getUser(), config.getPass());
        return con;
    }

    private void execute(Statement statement, String sql, PrestoResultSetBinder rsb, Connection con) throws SQLException {
        try {
            ResultSet rs = statement.executeQuery(sql);
            rsb.setPrestoResultSet((PrestoResultSet) rs);
            while (rs.next()) {
                if (rsb != null) {
                    rsb.bind(rs);
                }
            }
        } finally {
            close(statement, con);
        }
    }

}

