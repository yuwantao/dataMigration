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
//        migrateFrom201To203WithBlack2();
//        migrateFrom201To203WithWhite();
//        migrateFrom189To203WithBlack2();
//        migrateFrom189To203WithWhite();
//        testDBConnection();
//        migrateFrom202To203WithBlack1();
//        migrateFrom202To203WithGapp();
//        log("" + new Date().getTime());

        //migrate sensitive word
//        migrateSensitiveWord();
//        migrateSensitiveWordAlias();
        insertIntoPrs_Gapp_Doc_Content();
    }

    private static void insertIntoPrs_Gapp_Doc_Content() {
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/DEV_CIBTC_PRS";

        String selectGoodsStr = "select isbn from PRS_GAPP_BOOK";
        String insertPrsGappBookStr = "insert into PRS_GAPP_DOC_CONTENT(DOC_ID, ISBN, STATUS, CREATE_TIME, REMARK) values(?, ?, ?, ?,?)";

        try (
                Connection conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
                PreparedStatement selectGoodsStmt = conn203Yuwt.prepareStatement(selectGoodsStr);
                PreparedStatement insertPrsGappBookStmt = conn203Yuwt.prepareStatement(insertPrsGappBookStr);
        ) {
            conn203Yuwt.setAutoCommit(false);

            ResultSet rs = selectGoodsStmt.executeQuery();
            while (rs.next()) {
                insertPrsGappBookStmt.setLong(1, 18);
                insertPrsGappBookStmt.setString(2, rs.getString("isbn"));
                insertPrsGappBookStmt.setInt(3, 0);
                insertPrsGappBookStmt.setLong(4, new Date().getTime());
                insertPrsGappBookStmt.setString(5, "图书");
                insertPrsGappBookStmt.executeUpdate();
            }

            conn203Yuwt.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void migrateSensitiveWordAlias() {

    }

    private static void migrateSensitiveWord() {
        //                Connection conn202 = null;
        Properties connProperties202 = new Properties();
        connProperties202.put("user", "admin");
        connProperties202.put("password", "admin");
        String connStr202 = "jdbc:mysql://192.168.10.202:3306/product_center";

//                Connection conn203Yuwt = null;
//        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";

        String selectGoodsStr = "select sensitive_word from new_sensitive_word";
        String insertPrsGappBookStr = "insert into PRS_SENSITIVE_WORD(GROUP_ID, TYPE, FLITER_SCOPE, CREATE_BY, CREATE_TIME, FLITER_NAME) values(?, ?, ?, ?, ?,?)";

        try (
                Connection conn202 = DriverManager.getConnection(connStr202, connProperties202);
                Connection conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
                PreparedStatement selectGoodsStmt = conn202.prepareStatement(selectGoodsStr);
                PreparedStatement insertPrsGappBookStmt = conn203Yuwt.prepareStatement(insertPrsGappBookStr);
        ) {
            conn202.setAutoCommit(false);
            conn203Yuwt.setAutoCommit(false);

            ResultSet rs = selectGoodsStmt.executeQuery();
            while (rs.next()) {
                insertPrsGappBookStmt.setLong(1, 42);
                insertPrsGappBookStmt.setInt(2, 1);
                insertPrsGappBookStmt.setString(3, "1,2,3,4,5");
                insertPrsGappBookStmt.setString(4, "admin");
                insertPrsGappBookStmt.setLong(5, new Date().getTime());
                insertPrsGappBookStmt.setString(6, rs.getString("isbn"));
                insertPrsGappBookStmt.executeUpdate();
            }

            conn202.setAutoCommit(true);
            conn203Yuwt.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        log("migrate to gapp succeeded.");
    }

    private static void migrateFrom202To203WithGapp() {
        //                Connection conn202 = null;
        Properties connProperties202 = new Properties();
        connProperties202.put("user", "admin");
        connProperties202.put("password", "admin");
        String connStr202 = "jdbc:mysql://192.168.10.202:3306/product_center";

//                Connection conn203Yuwt = null;
//        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";

        String selectGoodsStr = "select isbn from black_list where source = 3";
        String insertPrsGappBookStr = "insert into PRS_GAPP_BOOK(isbn) values(?)";

        try (
                Connection conn202 = DriverManager.getConnection(connStr202, connProperties202);
                Connection conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
                PreparedStatement selectGoodsStmt = conn202.prepareStatement(selectGoodsStr);
                PreparedStatement insertPrsGappBookStmt = conn203Yuwt.prepareStatement(insertPrsGappBookStr);
        ) {
            conn202.setAutoCommit(false);
            conn203Yuwt.setAutoCommit(false);

            ResultSet rs = selectGoodsStmt.executeQuery();
            while (rs.next()) {
                insertPrsGappBookStmt.setString(1, rs.getString("isbn"));
                insertPrsGappBookStmt.executeUpdate();
            }

            conn202.setAutoCommit(true);
            conn203Yuwt.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        log("migrate to gapp succeeded.");
    }

    private static void migrateFrom202To203WithBlack1() {
//                Connection conn202 = null;
        Properties connProperties202 = new Properties();
        connProperties202.put("user", "admin");
        connProperties202.put("password", "admin");
        String connStr202 = "jdbc:mysql://192.168.10.202:3306/product_center";

//                Connection conn203Yuwt = null;
//        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";
        String connStr203Prs = "jdbc:mysql://192.168.10.203:3306/DEV_CIBTC_PRS";

//                Connection conn213 = null;
        Properties connProperties213 = new Properties();
        connProperties213.put("user", "hwlm");
        connProperties213.put("password", "dev@hwlm");
        String connStr213 = "jdbc:mysql://192.168.10.213:3306/CIBTC_PDC_16";

        String selectGoodsStr = "select isbn from black_list where source = 3";
        String selectGoodsStr189 = "select isbn,state from product where state = 1 limit ? offset ?";
        String selectMissedBooksIsbnStr = "select * from MISSED_BOOKS where isbn = ?";
        String selectPrsListBookIdStr = "select * from PRS_LIST where book_id = ?";
        String selectBookIdStr = "select book_id from BOOK where isbn = ?";
        String insertPrsListStr = "insert into PRS_LIST(book_id, isbn, type,  level, way, create_time) values(?, ?, ?, ?, ?, ?)";
        String insertPrsMissedBooksStr = "insert into MISSED_BOOKS(isbn, type, level, way) values(?, ?, ?, ?)";

        try (
                Connection conn202 = DriverManager.getConnection(connStr202, connProperties202);
                Connection conn213 = DriverManager.getConnection(connStr213, connProperties213);
                Connection conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
//                Connection conn203Prs = DriverManager.getConnection(connStr203Prs, connProperties203);
                PreparedStatement selectGoodsStmt = conn202.prepareStatement(selectGoodsStr);
                PreparedStatement selectMissedBooksIsbnStmt = conn203Yuwt.prepareStatement(selectMissedBooksIsbnStr);
                PreparedStatement selectPrsListBookIdStmt = conn203Yuwt.prepareStatement(selectPrsListBookIdStr);
                PreparedStatement selectBookIdStmt = conn213.prepareStatement(selectBookIdStr);
                PreparedStatement insertPrsListStmt = conn203Yuwt.prepareStatement(insertPrsListStr);
                PreparedStatement insertPrsMissedBooksStmt = conn203Yuwt.prepareStatement(insertPrsMissedBooksStr);
        ) {
            conn202.setAutoCommit(false);
            conn203Yuwt.setAutoCommit(false);
            conn213.setAutoCommit(false);
            ResultSet rs = selectGoodsStmt.executeQuery();
            while (rs.next()) {
//                    log(rs.getString("isbn"));
                String isbn = rs.getString("isbn");
//                    int goodsCheckState = rs.getInt("goods_check_state");
                int type;
                int level;
                int way;
                type = PrsType.BLACK.value;
                level = PrsLevel.ONE.value;
                way = PrsWay.ARTIFICIAL.value;
//
                selectBookIdStmt.setString(1, isbn);
                ResultSet rsBookId = selectBookIdStmt.executeQuery();
                if (!rsBookId.isBeforeFirst()) {
//                        log("isbn: " + isbn + " doesn't contained in PDC_16 BOOK table.");
                    selectMissedBooksIsbnStmt.setString(1, isbn);
                    insertPrsMissedBooksStmt.setString(1, isbn);
                    insertPrsMissedBooksStmt.setInt(2, type);
                    insertPrsMissedBooksStmt.setInt(3, level);
                    insertPrsMissedBooksStmt.setInt(4, way);
                    insertPrsMissedBooksStmt.executeUpdate();
//                    if (!selectMissedBooksIsbnStmt.executeQuery().isBeforeFirst()) {
//                    }
                }
                while (rsBookId.next()) {
                    long bookId = rsBookId.getLong(1);
                    selectPrsListBookIdStmt.setLong(1, bookId);
                    insertPrsListStmt.setLong(1, bookId);
//                        log("isbn: " + isbn + "; book id: " + rsBookId.getLong(1));
                    insertPrsListStmt.setString(2, isbn);
                    insertPrsListStmt.setInt(3, type);
                    insertPrsListStmt.setInt(4, level);
                    insertPrsListStmt.setInt(5, way);
                    insertPrsListStmt.setLong(6, new Date().getTime());
                    insertPrsListStmt.executeUpdate();
//                    if (!selectPrsListBookIdStmt.executeQuery().isBeforeFirst()) {
//                        //insert into prs_list
//                    }
                }
            }

            conn202.setAutoCommit(true);
            conn203Yuwt.setAutoCommit(true);
            conn213.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void migrateFrom189To203WithWhite() {
//        Connection conn201 = null;
        Properties connProperties201 = new Properties();
        connProperties201.put("user", "admin");
        connProperties201.put("password", "admin");
        String connStr201 = "jdbc:mysql://192.168.10.201:3306/readgo_center";

//        Connection conn189 = null;
        Properties connProperties189 = new Properties();
        connProperties189.put("user", "admin");
        connProperties189.put("password", "admin");
        String connStr189 = "jdbc:mysql://192.168.10.189:3306/readgoal";

//        Connection conn213 = null;
        Properties connProperties213 = new Properties();
        connProperties213.put("user", "hwlm");
        connProperties213.put("password", "dev@hwlm");
        String connStr213 = "jdbc:mysql://192.168.10.213:3306/CIBTC_PDC_16";

//        Connection conn203Yuwt = null;
//        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";
        String connStr203Prs = "jdbc:mysql://192.168.10.203:3306/DEV_CIBTC_PRS";


        long count = 0;
        String countQueryStr = "select count(*) from product where state = 1";
        try (
                Connection conn189 = DriverManager.getConnection(connStr189, connProperties189);
                Statement statement = conn189.createStatement();
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
        String selectGoodsStr = "select isbn,goods_check_state from goods_basic_info where goods_check_state = 1 limit ? offset ?";
        String selectGoodsStr189 = "select isbn,state from product where state = 1 limit ? offset ?";
        String selectMissedBooksIsbnStr = "select * from MISSED_BOOKS where isbn = ?";
        String selectPrsListBookIdStr = "select * from PRS_LIST where book_id = ?";
        String selectBookIdStr = "select book_id from BOOK where isbn = ?";
        String insertPrsListStr = "insert into PRS_LIST(book_id, isbn, type,  way, create_time) values(?, ?, ?, ?, ?)";
        String insertPrsMissedBooksStr = "insert into MISSED_BOOKS(isbn, type, way) values(?,  ?, ?)";

        while (offset < count) {
            try (
                    Connection conn201 = DriverManager.getConnection(connStr201, connProperties201);
                    Connection conn189 = DriverManager.getConnection(connStr189, connProperties189);
                    Connection conn213 = DriverManager.getConnection(connStr213, connProperties213);
                    Connection conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
                    Connection conn203Prs = DriverManager.getConnection(connStr203Prs, connProperties203);
                    PreparedStatement selectGoodsStmt = conn201.prepareStatement(selectGoodsStr);
                    PreparedStatement selectGoodsStmt189 = conn189.prepareStatement(selectGoodsStr189);
                    PreparedStatement selectMissedBooksIsbnStmt = conn203Yuwt.prepareStatement(selectMissedBooksIsbnStr);
                    PreparedStatement selectPrsListBookIdStmt = conn203Yuwt.prepareStatement(selectPrsListBookIdStr);
                    PreparedStatement selectBookIdStmt = conn213.prepareStatement(selectBookIdStr);
                    PreparedStatement insertPrsListStmt = conn203Yuwt.prepareStatement(insertPrsListStr);
                    PreparedStatement insertPrsMissedBooksStmt = conn203Yuwt.prepareStatement(insertPrsMissedBooksStr);
            ) {
                conn201.setAutoCommit(false);
                conn189.setAutoCommit(false);
                conn203Yuwt.setAutoCommit(false);
                conn203Prs.setAutoCommit(false);
                conn213.setAutoCommit(false);


                selectGoodsStmt189.setInt(1, limit);
                selectGoodsStmt189.setInt(2, offset);
                ResultSet rs = selectGoodsStmt189.executeQuery();
                rsIteration:
                while (rs.next()) {
//                    log(rs.getString("isbn"));
                    String isbn = rs.getString("isbn");
//                    int goodsCheckState = rs.getInt("goods_check_state");
                    int type;
                    int level;
                    int way;
                    type = PrsType.WHITE.value;
                    way = PrsWay.ARTIFICIAL.value;
//
                    selectBookIdStmt.setString(1, isbn);
                    ResultSet rsBookId = selectBookIdStmt.executeQuery();
                    if (!rsBookId.isBeforeFirst()) {
//                        log("isbn: " + isbn + " doesn't contained in PDC_16 BOOK table.");
                        selectMissedBooksIsbnStmt.setString(1, isbn);
                        if (!selectMissedBooksIsbnStmt.executeQuery().isBeforeFirst()) {
                            insertPrsMissedBooksStmt.setString(1, isbn);
                            insertPrsMissedBooksStmt.setInt(2, type);
                            insertPrsMissedBooksStmt.setInt(3, way);
                            insertPrsMissedBooksStmt.executeUpdate();
                        }
                    }
                    while (rsBookId.next()) {
                        long bookId = rsBookId.getLong(1);
                        selectPrsListBookIdStmt.setLong(1, bookId);
                        if (!selectPrsListBookIdStmt.executeQuery().isBeforeFirst()) {
                            //insert into prs_list
                            insertPrsListStmt.setLong(1, bookId);
//                        log("isbn: " + isbn + "; book id: " + rsBookId.getLong(1));
                            insertPrsListStmt.setString(2, isbn);
                            insertPrsListStmt.setInt(3, type);
                            insertPrsListStmt.setInt(4, way);
                            insertPrsListStmt.setLong(5, new Date().getTime());
                            insertPrsListStmt.executeUpdate();
                        }
                    }
                }
                offset += limit;

                conn201.setAutoCommit(true);
                conn189.setAutoCommit(true);
                conn203Yuwt.setAutoCommit(true);
                conn203Prs.setAutoCommit(true);
                conn213.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void migrateFrom201To203WithWhite() {
//        Connection conn201 = null;
        Properties connProperties201 = new Properties();
        connProperties201.put("user", "admin");
        connProperties201.put("password", "admin");
        String connStr201 = "jdbc:mysql://192.168.10.201:3306/readgo_center";

//        Connection conn213 = null;
        Properties connProperties213 = new Properties();
        connProperties213.put("user", "hwlm");
        connProperties213.put("password", "dev@hwlm");
        String connStr213 = "jdbc:mysql://192.168.10.213:3306/CIBTC_PDC_16";

//        Connection conn203Yuwt = null;
//        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";
        String connStr203Prs = "jdbc:mysql://192.168.10.203:3306/DEV_CIBTC_PRS";

//        PreparedStatement selectGoods = null;


        long count = 0;
        String countQueryStr = "select count(*) from goods_basic_info  where goods_check_state = 2";
        try (
                Connection conn201 = DriverManager.getConnection(connStr201, connProperties201);
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
        String selectGoodsStr = "select isbn,goods_check_state from goods_basic_info where goods_check_state = 2 limit ? offset ?";
        String selectBookIdStr = "select book_id from BOOK where isbn = ?";
        String insertPrsListStr = "insert into PRS_LIST(book_id, isbn, type, way, create_time) values(?, ?, ?, ?, ?)";
        String insertPrsMissedBooksStr = "insert into MISSED_BOOKS(isbn, type, way) values(?, ?, ?)";
        while (offset < count) {
        try (
                Connection conn201 = DriverManager.getConnection(connStr201, connProperties201);
                Connection conn213 = DriverManager.getConnection(connStr213, connProperties213);
                Connection conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
                Connection conn203Prs = DriverManager.getConnection(connStr203Prs, connProperties203);
                PreparedStatement selectGoodsStmt = conn201.prepareStatement(selectGoodsStr);
                PreparedStatement selectBookIdStmt = conn213.prepareStatement(selectBookIdStr);
                PreparedStatement insertPrsListStmt = conn203Yuwt.prepareStatement(insertPrsListStr);
                PreparedStatement insertPrsMissedBooksStmt = conn203Yuwt.prepareStatement(insertPrsMissedBooksStr);
        ) {
            conn201.setAutoCommit(false);
            conn203Yuwt.setAutoCommit(false);
            conn203Prs.setAutoCommit(false);
            conn213.setAutoCommit(false);



                selectGoodsStmt.setInt(1, limit);
                selectGoodsStmt.setInt(2, offset);
                ResultSet rs = selectGoodsStmt.executeQuery();
                rsIteration:
                while (rs.next()) {
//                    log(rs.getString("isbn"));
                    String isbn = rs.getString("isbn");
//                    int goodsCheckState = rs.getInt("goods_check_state");
                    int type;
                    int level;
                    int way;
                    type = PrsType.WHITE.value;
//                    level = PrsLevel.TWO.value;
                    way = PrsWay.ARTIFICIAL.value;
                    selectBookIdStmt.setString(1, isbn);
                    ResultSet rsBookId = selectBookIdStmt.executeQuery();
                    if (!rsBookId.isBeforeFirst()) {
//                        log("isbn: " + isbn + " doesn't contained in PDC_16 BOOK table.");
                        insertPrsMissedBooksStmt.setString(1, isbn);
                        insertPrsMissedBooksStmt.setInt(2, type);
                        insertPrsMissedBooksStmt.setInt(3, way);
                        insertPrsMissedBooksStmt.executeUpdate();
                    }
                    while (rsBookId.next()) {
                        //insert into prs_list
                        insertPrsListStmt.setLong(1, rsBookId.getLong(1));
//                        log("isbn: " + isbn + "; book id: " + rsBookId.getLong(1));
                        insertPrsListStmt.setString(2, isbn);
                        insertPrsListStmt.setInt(3, type);
                        insertPrsListStmt.setInt(4, way);
                        insertPrsListStmt.setLong(5, new Date().getTime());
                        insertPrsListStmt.executeUpdate();
                    }
                }
                offset += limit;
                conn201.setAutoCommit(true);
                conn203Yuwt.setAutoCommit(true);
                conn203Prs.setAutoCommit(true);
                conn213.setAutoCommit(true);
            }catch(SQLException e){
                e.printStackTrace();
            }
        }
    }

    private static void migrateFrom189To203WithBlack2() {
        Connection conn201 = null;
        Properties connProperties201 = new Properties();
        connProperties201.put("user", "admin");
        connProperties201.put("password", "admin");
        String connStr201 = "jdbc:mysql://192.168.10.201:3306/readgo_center";

        Connection conn189 = null;
        Properties connProperties189 = new Properties();
        connProperties189.put("user", "admin");
        connProperties189.put("password", "admin");
        String connStr189 = "jdbc:mysql://192.168.10.189:3306/readgoal";

        Connection conn213 = null;
        Properties connProperties213 = new Properties();
        connProperties213.put("user", "hwlm");
        connProperties213.put("password", "dev@hwlm");
        String connStr213 = "jdbc:mysql://192.168.10.213:3306/CIBTC_PDC_16";

        Connection conn203Yuwt = null;
        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";
        String connStr203Prs = "jdbc:mysql://192.168.10.203:3306/DEV_CIBTC_PRS";

//        PreparedStatement selectGoods = null;
        try {
            conn201 = DriverManager.getConnection(connStr201, connProperties201);
            conn189 = DriverManager.getConnection(connStr189, connProperties189);
            conn213 = DriverManager.getConnection(connStr213, connProperties213);
            conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
            conn203Prs = DriverManager.getConnection(connStr203Prs, connProperties203);
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
        String selectGoodsStr = "select isbn,goods_check_state from goods_basic_info where goods_check_state = 1 limit ? offset ?";
        String selectGoodsStr189 = "select isbn,state from product where state = 0 limit ? offset ?";
        String selectMissedBooksIsbnStr = "select * from MISSED_BOOKS where isbn = ?";
        String selectPrsListBookIdStr = "select * from PRS_LIST where book_id = ?";
        String selectBookIdStr = "select book_id from BOOK where isbn = ?";
        String insertPrsListStr = "insert into PRS_LIST(book_id, isbn, type, level, way, create_time) values(?, ?, ?, ?, ?, ?)";
        String insertPrsMissedBooksStr = "insert into MISSED_BOOKS(isbn, type, level, way) values(?, ?, ?, ?)";
        try (
                PreparedStatement selectGoodsStmt = conn201.prepareStatement(selectGoodsStr);
                PreparedStatement selectGoodsStmt189 = conn189.prepareStatement(selectGoodsStr189);
                PreparedStatement selectMissedBooksIsbnStmt = conn203Yuwt.prepareStatement(selectMissedBooksIsbnStr);
                PreparedStatement selectPrsListBookIdStmt = conn203Yuwt.prepareStatement(selectPrsListBookIdStr);
                PreparedStatement selectBookIdStmt = conn213.prepareStatement(selectBookIdStr);
                PreparedStatement insertPrsListStmt = conn203Yuwt.prepareStatement(insertPrsListStr);
                PreparedStatement insertPrsMissedBooksStmt = conn203Yuwt.prepareStatement(insertPrsMissedBooksStr);
        ) {
            conn201.setAutoCommit(false);
            conn189.setAutoCommit(false);
            conn203Yuwt.setAutoCommit(false);
            conn203Prs.setAutoCommit(false);
            conn213.setAutoCommit(false);

            while (offset < count) {
                selectGoodsStmt189.setInt(1, limit);
                selectGoodsStmt189.setInt(2, offset);
                ResultSet rs = selectGoodsStmt189.executeQuery();
                rsIteration: while (rs.next()) {
//                    log(rs.getString("isbn"));
                    String isbn = rs.getString("isbn");
//                    int goodsCheckState = rs.getInt("goods_check_state");
                    int type;
                    int level;
                    int way;
                    type = PrsType.BLACK.value;
                    level = PrsLevel.TWO.value;
                    way = PrsWay.ARTIFICIAL.value;
//
                    selectBookIdStmt.setString(1, isbn);
                    ResultSet rsBookId = selectBookIdStmt.executeQuery();
                    if (!rsBookId.isBeforeFirst()) {
//                        log("isbn: " + isbn + " doesn't contained in PDC_16 BOOK table.");
                        selectMissedBooksIsbnStmt.setString(1, isbn);
                        if (!selectMissedBooksIsbnStmt.executeQuery().isBeforeFirst()) {
                            insertPrsMissedBooksStmt.setString(1, isbn);
                            insertPrsMissedBooksStmt.setInt(2, type);
                            insertPrsMissedBooksStmt.setInt(3, level);
                            insertPrsMissedBooksStmt.setInt(4, way);
                            insertPrsMissedBooksStmt.executeUpdate();
                        }
                    }
                    while (rsBookId.next()) {
                        long bookId = rsBookId.getLong(1);
                        selectPrsListBookIdStmt.setLong(1, bookId);
                        if (!selectPrsListBookIdStmt.executeQuery().isBeforeFirst()) {
                            //insert into prs_list
                            insertPrsListStmt.setLong(1, bookId);
//                        log("isbn: " + isbn + "; book id: " + rsBookId.getLong(1));
                            insertPrsListStmt.setString(2, isbn);
                            insertPrsListStmt.setInt(3, type);
                            insertPrsListStmt.setInt(4, level);
                            insertPrsListStmt.setInt(5, way);
                            insertPrsListStmt.setLong(6, new Date().getTime());
                            insertPrsListStmt.executeUpdate();
                        }
                    }
                }
                offset += limit;
            }

            conn201.setAutoCommit(true);
            conn189.setAutoCommit(true);
            conn203Yuwt.setAutoCommit(true);
            conn203Prs.setAutoCommit(true);
            conn213.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
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

    private static void migrateFrom201To203WithBlack2() {
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

        Connection conn203Yuwt = null;
        Connection conn203Prs = null;
        Properties connProperties203 = new Properties();
        connProperties203.put("user", "root");
        connProperties203.put("password", "123456");
        String connStr203Yuwt = "jdbc:mysql://192.168.10.203:3306/yuwantao";
        String connStr203Prs = "jdbc:mysql://192.168.10.203:3306/DEV_CIBTC_PRS";

//        PreparedStatement selectGoods = null;
        try {
            conn201 = DriverManager.getConnection(connStr201, connProperties201);
            conn213 = DriverManager.getConnection(connStr213, connProperties213);
            conn203Yuwt = DriverManager.getConnection(connStr203Yuwt, connProperties203);
            conn203Prs = DriverManager.getConnection(connStr203Prs, connProperties203);
        } catch (SQLException e) {
            log("cannot establish db connection.");
        }

        long count = 0;
        String countQueryStr = "select count(*) from goods_basic_info where goods_check_state = 1";
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
        String selectGoodsStr = "select isbn,goods_check_state from goods_basic_info where goods_check_state = 1 limit ? offset ?";
        String selectBookIdStr = "select book_id from BOOK where isbn = ?";
        String insertPrsListStr = "insert into PRS_LIST(book_id, isbn, type, level, way, create_time) values(?, ?, ?, ?, ?, ?)";
        String insertPrsMissedBooksStr = "insert into MISSED_BOOKS(isbn, type, level, way) values(?, ?, ?, ?)";
        try (
                PreparedStatement selectGoodsStmt = conn201.prepareStatement(selectGoodsStr);
                PreparedStatement selectBookIdStmt = conn213.prepareStatement(selectBookIdStr);
                PreparedStatement insertPrsListStmt = conn203Yuwt.prepareStatement(insertPrsListStr);
                PreparedStatement insertPrsMissedBooksStmt = conn203Yuwt.prepareStatement(insertPrsMissedBooksStr);
        ) {
            conn201.setAutoCommit(false);
            conn203Yuwt.setAutoCommit(false);
            conn203Prs.setAutoCommit(false);
            conn213.setAutoCommit(false);

            while (offset < count) {
                selectGoodsStmt.setInt(1, limit);
                selectGoodsStmt.setInt(2, offset);
                ResultSet rs = selectGoodsStmt.executeQuery();
                rsIteration: while (rs.next()) {
//                    log(rs.getString("isbn"));
                    String isbn = rs.getString("isbn");
//                    int goodsCheckState = rs.getInt("goods_check_state");
                    int type;
                    int level;
                    int way;
                    type = PrsType.BLACK.value;
                    level = PrsLevel.TWO.value;
                    way = PrsWay.ARTIFICIAL.value;
//
//                    switch (goodsCheckState) {
//                        case 0:
//                            continue rsIteration;
//                        case 1:
//                            type = PrsType.BLACK.value;
//                            level = PrsLevel.TWO.value;
//                            way = PrsWay.ARTIFICIAL.value;
//                            break;
//                        case 2:
//                            type = PrsType.WHITE.value;
//                            level = 3;
//                            way = PrsWay.ARTIFICIAL.value;
//                            break;
//                        case 3:
//                            continue rsIteration;
//                        default:
//                            continue rsIteration;
//                    }
                    selectBookIdStmt.setString(1, isbn);
                    ResultSet rsBookId = selectBookIdStmt.executeQuery();
                    if (!rsBookId.isBeforeFirst()) {
//                        log("isbn: " + isbn + " doesn't contained in PDC_16 BOOK table.");
                        insertPrsMissedBooksStmt.setString(1, isbn);
                        insertPrsMissedBooksStmt.setInt(2, type);
                        insertPrsMissedBooksStmt.setInt(3, level);
                        insertPrsMissedBooksStmt.setInt(4, way);
                        insertPrsMissedBooksStmt.executeUpdate();
                    }
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
            conn203Yuwt.setAutoCommit(true);
            conn203Prs.setAutoCommit(true);
            conn213.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void log(String message) {
        System.out.println(message);
    }
}
