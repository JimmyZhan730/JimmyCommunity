package com.nowcoder.community;

import java.io.IOException;

public class WkTests {

    public static void main(String[] args) {
        String cmd = "d:/01JWorkSpace/wkhtmltopdf/bin/wkhtmltoimage --quality 75 https://www.programmercarl.com d:/01JWorkSpace/data/wk-images/3.png";
        try {
            Runtime.getRuntime().exec(cmd);
            System.out.println("ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
