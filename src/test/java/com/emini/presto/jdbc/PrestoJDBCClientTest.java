package com.emini.presto.jdbc;

import com.emini.presto.config.JDBCConfig;
import com.emini.presto.resultset.PrestoResultSetBinder;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

@Ignore
public class PrestoJDBCClientTest {
    private static PrestoJDBCClient client = null;

    @Before
    public void setUp() {
        JDBCConfig jdbcConfig = new JDBCConfig("jdbc:presto://host:port/hive/default",
                "emin",
                "1234");
        client = new PrestoJDBCClient(jdbcConfig);
    }


    @Test
    public void testJDBCClient() throws SQLException {
        String sql = "select * from table limit 1";
        client.executeQuery(null, "user", sql, new PrestoResultSetBinder() {
                    @Override
                    public void bind(ResultSet rs) throws SQLException {
                        Assert.assertEquals("test", rs.getString("testcolumn"));
                    }
                }
        );
    }
}
