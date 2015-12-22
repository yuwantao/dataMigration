package com.cibtc.yuwt.datamigration;

/**
 * Created by yuwt on 2015/12/22.
 */
public class Book {
    private int id;
    private int pdcId;
    private String isbn;
    private int state;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPdcId() {
        return pdcId;
    }

    public void setPdcId(int pdcId) {
        this.pdcId = pdcId;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
