/**
 * Created by Administrator on 2018/1/1.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;

//@Override
public class callNoReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{

        double sum = 1.0;
        double day_num = 1.0;

        Iterator<IntWritable> iterator = values.iterator();

        int day_id = iterator.next().get();

        while (iterator.hasNext()) {

            sum += 1 ;  //用户通话总次数
            int day_id_new = iterator.next().get();
            if (day_id != day_id_new){
                day_num += 1;
                day_id = day_id_new;
            }
//            val = iterator.next();
        }
        // sum: total callNum for each calling_nbr in a row
        sum = sum / day_num;
        context.write(key, new DoubleWritable(sum));
    }

}
