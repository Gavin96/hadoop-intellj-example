/**
 * Created by Administrator on 2018/1/3.
 */
import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class callTimePeriodMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line0 = value.toString();
        String line[] = line0.split("[\t]+");

        String calling_nbr =line[1];
        String start_time = line[9];
        String end_time = line[10];
        String raw_dur = line[11];

        String[] washS = start_time.split(":");
        int hourS = Integer.parseInt(washS[0]);
        int minS = Integer.parseInt(washS[1]);
        int secS = Integer.parseInt(washS[2]);
        long totalSecS = hourS*60*60 + minS*60 + secS;

        String[] washE = end_time.split(":");
        int hourE = Integer.parseInt(washE[0]);
        int minE = Integer.parseInt(washE[1]);
        int secE = Integer.parseInt(washE[2]);
        long totalSecE = hourE*60*60 + minE*60 + secE;
        int raw_durNow = Integer.parseInt(raw_dur);
        boolean good = false;
        if (totalSecS + raw_durNow == totalSecE){
            // 聊天过程没跨过24:00:00
            good = true;
        } else if(totalSecS + raw_durNow >= 24*60*60){
            // 聊天过程跨过了24:00:00
            if (totalSecS + raw_durNow - 24*60*60 == totalSecE){
                good = true;
            }
        }

        if (good){
            context.write(new Text(calling_nbr), new Text(start_time + ',' + end_time + ',' + raw_dur));
        }
    }
}
