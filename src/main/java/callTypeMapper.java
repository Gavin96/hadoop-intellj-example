/**
 * Created by Administrator on 2018/1/2.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import java.util.StringTokenizer;

public class callTypeMapper extends Mapper<Object, Text, Text, IntWritable> {

//    public static final IntWritable one = new IntWritable(1);
//    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line0 = value.toString();
        String line[] = line0.split("[\t]+");

        String call_type = line[12]; //13rd attribute
//        int calling_optr = Integer.parseInt(line[3]);  //4th attribute
        int called_optr = Integer.parseInt(line[4]);  //5th attribute

        context.write(new Text(call_type), new IntWritable(called_optr));
    }
}
