package com.cibtc.yuwt.datamigration;

import java.sql.*;
import java.util.Date;
import java.util.Properties;

/**
 * Created by yuwt on 2015/12/22.
 */
public class DataMigration {
    static enum PrsWay {
        PROGRAM(1), ARTIFICIAL(2);

        private final int value;
        PrsWay(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }

    static enum PrsLevel {
        ONE(1), TWO(2);

        private final int value;

        PrsLevel(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }

    static enum PrsType {
        BLACK(1), WHITE(2), GREY(3), GREEN(4);

        private final int value;

        PrsType(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }


    public static void main(String... args) {
        migrateFrom201To203();
//        testDBConnection();
    }

    private static void testDBConnection() {
        Connection conn203 = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203 = "jdbc:mysql://192.168.10.203:3306/yuwantao";

        Connection conn213 = null;
        Properties connProperties213 = new Properties();
        connProperties213.put("user", "hwlm");
        connProperties213.put("password", "dev@hwlm");
        String connStr213 = "jdbc:mysql://192.168.10.213:3306/CIBTC_PDC_16";

        try {
//            conn201 = DriverManager.getConnection(connStr201, connProperties201);
            conn213 = DriverManager.getConnection(connStr213, connProperties213);
//            conn203 = DriverManager.getConnection(connStr203, connProperties203);
        } catch (SQLException e) {
            log("cannot establish db connection.");
        }

        String stmtStr = "select count(*) from BOOK";
        try(
                PreparedStatement stmt = conn213.prepareStatement(stmtStr);
        ){
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                log("" + rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void migrateFrom201To203() {
        Connection conn201 = null;
        Properties connProperties201 = new Properties();
        connProperties201.put("user", "admin");
        connProperties201.put("password", "admin");
        String connStr201 = "jdbc:mysql://192.168.10.201:3306/readgo_center";

        Connection conn213 = null;
        Properties connProperties213 = new Properties();
        connProperties213.put("user", "hwlm");
        connProperties213.put("password", "dev@hwlm");
        String connStr213 = "jdbc:mysql://192.168.10.213:3306/CIBTC_PDC_16";

        Connection conn203 = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203 = "jdbc:mysql://192.168.10.203:3306/yuwantao";

//        PreparedStatement selectGoods = null;
        try {
            conn201 = DriverManager.getConnection(connStr201, connProperties201);
            conn213 = DriverManager.getConnection(connStr213, connProperties213);
            conn203 = DriverManager.getConnection(connStr203, connProperties203);
        } catch (SQLException e) {
            log("cannot establish db connection.");
        }

        long count = 0;
        String countQueryStr = "select count(*) from goods_basic_info";
        try (
                Statement statement = conn201.createStatement();
        ){
            ResultSet rs = statement.executeQuery(countQueryStr);
            if (rs.next()) {
                count = rs.getInt(1);
            }
            log("goods_basic_info count: " + count);
        } catch (SQLException e) {
            log("get count from goods_basic_info failed.");
        }

        int limit = 10000;
        int offset = 0;
        String selectGoodsStr = "select isbn,goods_check_state from goods_basic_info limit ? offset ?";
        String selectBookIdStr = "select book_id from BOOK where isbn = ?";
        String insertPrsListStr = "insert into PRS_LIST(book_id, isbn, type, level, way, create_time) values(?, ?, ?, ?, ?, ?)";
        try (
                PreparedStatement selectGoodsStmt = conn201.prepareStatement(selectGoodsStr);
                PreparedStatement selectBookIdStmt = conn213.prepareStatement(selectBookIdStr);
                PreparedStatement insertPrsListStmt = conn203.prepareStatement(insertPrsListStr);
        ) {
            conn201.setAutoCommit(false);
            conn203.setAutoCommit(false);
            conn213.setAutoCommit(false);

            while (offset < count) {
                selectGoodsStmt.setInt(1, limit);
                selectGoodsStmt.setInt(2, offset);
                ResultSet rs = selectGoodsStmt.executeQuery();
                rsIteration: while (rs.next()) {
//                    log(rs.getString("isbn"));
                    String isbn = rs.getString("isbn");
                    int goodsCheckState = rs.getInt("goods_check_state");
                    int type;
                    int level;
                    int way;
                    switch (goodsCheckState) {
                        case 0:
                            continue rsIteration;
                        case 1:
                            type = PrsType.BLACK.value;
                            level = PrsLevel.TWO.value;
                            way = PrsWay.ARTIFICIAL.value;
                            break;
                        case 2:
                            continue rsIteration;
                        case 3:
                            continue rsIteration;
                        default:
                            continue rsIteration;
                    }
                    selectBookIdStmt.setString(1, isbn);
                    ResultSet rsBookId = selectBookIdStmt.executeQuery();
                    while (rsBookId.next()) {
                        //insert into prs_list
                        insertPrsListStmt.setLong(1, rsBookId.getLong(1));
//                        log("isbn: " + isbn + "; book id: " + rsBookId.getLong(1));
                        insertPrsListStmt.setString(2, isbn);
                        insertPrsListStmt.setInt(3, type);
                        insertPrsListStmt.setInt(4, level);
                        insertPrsListStmt.setInt(5, way);
                        insertPrsListStmt.setLong(6, new Date().getTime());
                        insertPrsListStmt.executeUpdate();
                    }
                }
                offset += limit;
            }

            conn201.setAutoCommit(true);
            conn203.setAutoCommit(true);
            conn213.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void log(String message) {
        System.out.println(message);
    }
}
