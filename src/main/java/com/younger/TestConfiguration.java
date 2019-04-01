package com.younger;

import org.apache.hadoop.conf.Configuration;

public class TestConfiguration {
    public static void main(String[] args) {
        Configuration entries = new Configuration();
        entries.addResource("test.xml");

        System.out.println(entries.get("top.n"));
        System.out.println(entries.get("中国你好！"));
    }
}
