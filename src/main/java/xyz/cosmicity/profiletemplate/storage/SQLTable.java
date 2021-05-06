package xyz.cosmicity.profiletemplate.storage;

import co.aikar.idb.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SQLTable {

    private final String name, cols, colsJoined;
    private final String[] pk;

    public SQLTable(final String table, final String pkLbl, final String pkType, final String columns) {
        name = table;
        pk = new String[]{pkLbl, pkType};
        cols = columns;
        colsJoined = joinCols();
        init();
    }

    public String joinCols() {
        String[] split = cols.split(" ");
        List<String> columns = new ArrayList<>();
        for(int i = 0; i < split.length; i++) {
            if( i % 2 == 1) {// i is odd
                columns.add(split[i - 1] + " " + split[i]);
            }
        }
        return String.join(", ", columns.toArray(String[]::new));
    }

    public void init() {
        SQLUtils.update("CREATE TABLE IF NOT EXISTS "+ name
                + " (" + String.join(" ",pk) +" PRIMARY KEY, "+ colsJoined + ");");
    }

    public String getName() {
        return name;
    }

    public String getCols() {
        return cols;
    }

    public List<String> getColLabels() {
        List<String> labels = new ArrayList<>();
        for(int i = 0; i < cols.split(" ").length; i++) {
            if( i % 2 == 0) {// even is label
                labels.add(cols.split(" ")[i]);
            }
        }
        return labels;
    }

    public String[] getPk() {
        return pk;
    }

    public String getPkLabel() {
        return pk[0];
    }

    public Object getRow(Database db, final String key) {
        List<Object> objects = new ArrayList<>();

        try(Connection con = db.getConnection();
            PreparedStatement pst =
                    con.prepareStatement("SELECT * FROM "+name+" WHERE "+getPkLabel()+"="+key+";");
            ResultSet rs = pst.executeQuery();) {
            if(cols.split(" ").length == 0) {
                int i = 1;
                while(rs.next()) {
                    objects.add(rs.getObject(i));
                    i++;
                }
            }
            else {
                while (rs.next()) {
                    for(int i = 0; i < cols.split(" ").length; i++) {
                        if( i % 2 == 0) {// even is label
                            objects.add(rs.getObject(cols.split(" ")[i]));
                        }
                    }
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return objects;
    }

}
