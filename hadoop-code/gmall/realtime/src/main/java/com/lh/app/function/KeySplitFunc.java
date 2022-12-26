package com.lh.app.function;

import com.lh.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 15:38 2022/11/16
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeySplitFunc extends TableFunction<Row> {
    public void eval(String str) throws IOException {
        List<String> words = KeywordUtil.splitKeyWord(str);
        for (String s : words) {
            collect(Row.of(s));
        }
    }
}
