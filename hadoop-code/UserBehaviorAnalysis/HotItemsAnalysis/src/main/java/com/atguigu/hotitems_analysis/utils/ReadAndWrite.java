package com.atguigu.hotitems_analysis.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 12:01 2021/10/15
 */


public class ReadAndWrite {

    private static BufferedWriter bufferedWriter;

    public static void readFile(String FileName){

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(FileName));

            String line = null;
            while ((line=bufferedReader.readLine()) != null){
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeFile(String Cont) throws IOException {

        if(bufferedWriter == null){
            bufferedWriter = new BufferedWriter(new FileWriter("D:\\workLv\\learn\\proj\\hadoop-code" +
                    "\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\out.txt"));
            bufferedWriter.write(Cont);
        }else {
            bufferedWriter.write(Cont);
        }
        bufferedWriter.flush();

    }

    public static void main(String[] args) throws IOException {

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("D:\\workLv\\learn\\proj\\hadoop-code" +
                "\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\out.txt"));
        bufferedWriter.write("aa");
        bufferedWriter.write("cc");
        bufferedWriter.flush();
        bufferedWriter.close();
    }
}


