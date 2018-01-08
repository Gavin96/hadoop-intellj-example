/**
 * Created by Administrator on 2018/1/1.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import java.util.StringTokenizer;

public class callNoMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

//    StringTokenizer itr = new StringTokenizer(value.toString());
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

//        StringTokenizer itr = new StringTokenizer(value.toString());

        String line0 = value.toString();
        String line[] = line0.split("[\t]+");

        int day_id = Integer.parseInt(line[0]);
        String calling_nbr = line[1];
//        if (itr.hasMoreTokens()){
            context.write(new Text(calling_nbr), new IntWritable(day_id));
//        }
    }
}
