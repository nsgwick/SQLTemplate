package xyz.cosmicity.profiletemplate.storage;

import co.aikar.idb.Database;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SQLUtils {

    static Database db;

    public static void setDb(Database dbase) {
        db = dbase;
    }
    public static Database getDb() {
        return db;
    }
    public static void close() {
        db.close();
    }

    public static List<Object> getRow(@NotNull final SQLTable table, @NotNull final String key, @NotNull final String... columns) {

        List<Object> objects = new ArrayList<>();

        try (Connection con = db.getConnection();
             PreparedStatement pst = con.prepareStatement("SELECT * FROM " + table.getName() + " WHERE "+table.getPkLabel()+"="+key+";");
             ResultSet rs = pst.executeQuery();){

            if(columns.length == 0) {
                int i=1;
                while (rs.next()) {
                    objects.add(rs.getObject(i));
                    i++;
                }
            }
            else {
                while(rs.next()) {
                    for(String s : columns) {
                        objects.add(rs.getObject(s));
                    }
                }
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return objects;
    }

    /**
     * @param values - the values only. (just values not colLabel=value etc)
     */
    public static void setRow(final SQLTable table, final String key, final String... values) {
        List<String> columns = new ArrayList<>(),
                tableColumns = table.getColLabels(),
        equivalents = new ArrayList<>();
        for(int i = 0; i < values.length; i ++) {
            columns.add(tableColumns.get(i));
            equivalents.add(tableColumns.get(i) + " = " + values[i]);
        }
        update(
                "INSERT INTO "+ table.getName() +
                        " (" + table.getPkLabel() + "," + String.join(",",columns) + ")" +
                        " VALUES (" + key + "," + String.join(String.join(",", values)) + ")" +
                        " ON DUPLICATE KEY UPDATE " + String.join(", ",equivalents) + ";");

        // INSERT INTO profiles (uuid, joined, discordid) VALUES ("rehwjalnkj", 3849214798, "") ON DUPLICATE UPDATE joined = 432498234, discordid = ""
    }

    public static void update(final String query) {
        try (Connection con = db.getConnection();
             PreparedStatement pst = con.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void update(@NotNull final String SQL_QUERY, Object... objects) {
        try (Connection con = db.getConnection();
             PreparedStatement pst = con.prepareStatement(SQL_QUERY);) {

            for(int i=1;i < objects.length;i++) {
                pst.setObject(i,objects[i]);
            }
            pst.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static boolean holdsKey(final SQLTable table, final String key) {
        int size = 0;

        try(
                Connection con = db.getConnection();
                PreparedStatement pst = con.prepareStatement("SELECT * FROM "+ table.getName() +" WHERE "+ table.getPkLabel()+"="+key+";");
                ResultSet rs = pst.executeQuery()
        ) {
            if(rs != null) {
                size = rs.getRow();
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return size>0;
    }
}
