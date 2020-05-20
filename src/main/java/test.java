import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) throws IOException {
        File file = new File("F:\\spark-数据清洗-数据仓库\\计嵌2班-Spark-数据清洗-数据仓库\\任务\\jobs1.csv");
        InputStream in = new FileInputStream(file);
        CsvReader cr = new CsvReader(in, Charset.forName("utf-8"));

        String outFile = "F:\\spark-数据清洗-数据仓库\\计嵌2班-Spark-数据清洗-数据仓库\\任务\\jobs11.csv";
        CsvWriter cs = new CsvWriter(outFile,',', Charset.forName("utf-8"));

        cr.readHeaders();
        int a = 0;
 //       System.out.println(cr.getHeader(12));
 //       System.out.println(Arrays.toString(cr.getHeaders()));
        while(cr.readRecord()) {
            String RawRecord = cr.getRawRecord();
            if(RawRecord.startsWith("company_financing_stage")){
                continue;
            }
            String rawRecord = cr.getRawRecord();
            System.out.println(rawRecord);
            int columnCount = cr.getColumnCount();
            System.out.println(columnCount);
            for (int i = 0; i < columnCount; i++) {
                String str = cr.get(i);
                Pattern p = Pattern.compile("\\s+\\t+\\n+\\r");
                Matcher m = p.matcher(str);
                String s = m.replaceAll("~~");
                System.out.println(s);
            }
            System.out.println("\n");
        }
//        System.out.println(a);
        cr.close();
    }
}
