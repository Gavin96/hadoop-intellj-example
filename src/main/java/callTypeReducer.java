/**
 * Created by Administrator on 2018/1/2.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;


public class callTypeReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{

        double callTimesSum = 0;

        Iterator<IntWritable> iterator = values.iterator();

        double telecomNum = 0.0;
        double mobileNum = 0.0;
        double unicomNum = 0.0;

        while (iterator.hasNext()){

            callTimesSum ++;
            int called_optr = iterator.next().get();

            if (called_optr == 1){
                telecomNum ++;   //电信
            }else if(called_optr == 2){
                mobileNum ++;  //移动
            }else if(called_optr == 3){
                unicomNum ++;  //联通
            }
        }

        double telecomRate = telecomNum / callTimesSum;
        double mobileRate = mobileNum / callTimesSum;
        double unicomRate = unicomNum / callTimesSum;

        context.write(key, new DoubleWritable(telecomRate));  //key下的电信占比
        context.write(key, new DoubleWritable(mobileRate));   //key下的移动占比
        context.write(key, new DoubleWritable(unicomRate));   //key下的联通占比
    }

}
