package com.ym.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**分词工具类
 * @author yomo
 * @create 2021-05-06 8:37
 */
public class KeywordUtil {

    public static List<String> analyze(String keyWord) {

        //定义集合
        ArrayList<String> list = new ArrayList<>();

        //创建Reader
        StringReader reader = new StringReader(keyWord);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        Lexeme next = null;
        try {
            next = ikSegmenter.next();

            while (next != null) {

                //将分出的词加入集合
                list.add(next.getLexemeText());
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //返回结果
        return list;

    }

    //测试
    public static void main(String[] args) {
        System.out.println(analyze("大数据项目之Flink实时数仓"));
    }

}
