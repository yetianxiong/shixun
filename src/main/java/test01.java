import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class test01 {
    public static void main(String[] args) throws IOException {
        File file = new File("F:\\spark-数据清洗-数据仓库\\计嵌2班-Spark-数据清洗-数据仓库\\任务\\jobs1.csv");
        InputStream in = new FileInputStream(file);
        CsvReader cr = new CsvReader(in, Charset.forName("utf-8"));
        // 读表头
//       cr.readHeaders();
        String file2 = "F:\\spark-数据清洗-数据仓库\\计嵌2班-Spark-数据清洗-数据仓库\\任务\\jobs11.csv";
        // 创建CSV写对象
        CsvWriter cs = new CsvWriter(file2,',', Charset.forName("utf-8"));
        while(cr.readRecord()) {
            String RawRecord = cr.getRawRecord();
            if(RawRecord.startsWith("company_financing_stage")){
                continue;
            }
            int columnCount = cr.getColumnCount();
            String[] s = new String[cr.getColumnCount()];
            for (int i = 0; i <columnCount; i++) {
                if(i==11){
                    String str = cr.get(i);
                    str = str.replaceAll("k", "");//小写
                    str = str.replaceAll("K", "");//大写

                    String[] n = str.split("-");
                    float n0 = Float.parseFloat(n[0]);
                    float n1 = Float.parseFloat(n[1]);
                    float salary = (n0 + n1) / 2;
                    s[i] = String.valueOf((int)salary);
                }else {
                    String str = cr.get(i);
                    s[i] = str;
                }
            }
            cs.writeRecord(s);
        }
        cs.close();
    }
}