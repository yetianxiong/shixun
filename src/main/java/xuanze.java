import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class xuanze {
    public static void main(String[] args) throws IOException {
        File file = new File("F:\\spark-数据清洗-数据仓库\\计嵌2班-Spark-数据清洗-数据仓库\\任务\\jobs11.csv");
        InputStream in = new FileInputStream(file);
        CsvReader cr = new CsvReader(in, Charset.forName("utf-8"));
        // 读表头
//        cr.readHeaders();
        String file2 = "F:\\spark-数据清洗-数据仓库\\计嵌2班-Spark-数据清洗-数据仓库\\任务\\job1.txt";
        // 创建CSV写对象
        CsvWriter cs = new CsvWriter(file2,',', Charset.forName("utf-8"));
        while(cr.readRecord()) {
/*            String RawRecord = cr.getRawRecord();
            if(RawRecord.startsWith("company_financing_stage")){
                continue;
            }*/
            int[] a = {1,2,7,8,11};
            String[] s = new String[5];
            for (int i = 0; i <a.length; i++) {
                s[i] = cr.get(a[i]);
            }
            cs.writeRecord(s);
        }
        cs.close();
    }
}
