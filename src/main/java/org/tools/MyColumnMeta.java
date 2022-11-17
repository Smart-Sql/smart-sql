package org.tools;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.jdbc.thin.JdbcThinResultSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class MyColumnMeta {

    public static Map<String, Integer> getColumnMeta(String sql) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true&userToken=dafu";
        Connection conn = DriverManager.getConnection(url);

        PreparedStatement stmt = conn.prepareStatement(sql);

        JdbcThinResultSet rs = (JdbcThinResultSet) stmt.executeQuery();
        Map<String, Integer> ht = rs.columnOrder();

        rs.close();
        stmt.close();
        conn.close();
        return ht;
    }

    public static int getColumnCount(Ignite ignite, String sql) {
        List<List<?>> lst = ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
        return MyConvertUtil.ConvertToInt(lst.get(0).get(0));
    }

    public static String getColumnRow(Ignite ignite, String sql, Hashtable<String, Object> ps) throws ClassNotFoundException, SQLException {
        List<HashMap<String, String>> rs = new ArrayList<>();
        Map<String, Integer> ht = getColumnMeta(sql);
        List<List<?>> lst = ignite.cache("public_meta").query(new SqlFieldsQuery(sql + " LIMIT " + MyConvertUtil.ConvertToInt(ps.get("start")) + "," + MyConvertUtil.ConvertToInt(ps.get("limit")))).getAll();
        for (List<?> row : lst)
        {
            HashMap<String, String> map = new HashMap<>();
            for (String key : ht.keySet())
            {
                map.put(key, row.get(ht.get(key)).toString());
            }
            rs.add(map);
        }

        return MyGson.groupObjToLine(rs);
    }

    public static List<HashMap<String, String>> getColumnRow(Map<String, Integer> ht,  List<List<?>> lst)
    {
        List<HashMap<String, String>> rs = new ArrayList<>();
        for (List<?> row : lst)
        {
            HashMap<String, String> map = new HashMap<>();
            for (String key : ht.keySet())
            {
                map.put(key, row.get(ht.get(key)).toString());
            }
            rs.add(map);
        }
        return rs;
    }
}
