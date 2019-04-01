package com.younger;

public class PageCount implements Comparable<PageCount> {
    private String page;
    private int count;

    public void set(String page,int count){
        this.page = page;
        this.count = count;
    }

    public void setPage(String page){
        this.page = page;
    }

    public String getPage(){
        return this.page;
    }

    public void setCount(int count){
        this.count = count;
    }

    public int getCount(){
        return this.count;
    }

    @Override
    public int compareTo(PageCount o) {
        return o.getCount()-this.count==0?this.page.compareTo(o.getPage()):o.getCount()-this.count;
    }
}
