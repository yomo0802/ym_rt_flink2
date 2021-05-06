package com.ym.realtime.app.funcs;

import com.ym.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.security.Key;
import java.util.List;

/** UDTF 一对多 相当于MySQL 炸裂函数
 * @author yomo
 * @create 2021-05-06 8:41
 */
// FunctionHint 标识输出数据的类型
// row.setField(0,keyword)中的0表示返回值下标为0的值
@FunctionHint(output =  @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {

    public void eval(String str) {

        //使用IK分词器对搜索的关键词进行分词处理
        List<String> list = KeywordUtil.analyze(str);

        //遍历单词写出
        for (String word : list) {
            collect(Row.of(word));
        }
    }
}
