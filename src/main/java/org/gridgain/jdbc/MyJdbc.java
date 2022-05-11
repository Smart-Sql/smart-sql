package org.gridgain.jdbc;

import java.sql.*;

public class MyJdbc {
    public static Boolean hasConnPermission(final String userToken) {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcDriver");
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
            Statement stmt = conn.createStatement();
            //ResultSet rs = stmt.executeQuery("SELECT hasConnPermission('"+ userToken +"')");
            ResultSet rs = stmt.executeQuery("hasConnPermission('"+ userToken +"')");
            Boolean hasPermission = false;
            while (rs.next())
            {
                hasPermission = rs.getBoolean(1);
            }
            rs.close();
            return hasPermission;
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
