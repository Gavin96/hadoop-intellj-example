/**
 * Created by Administrator on 2018/1/3.
 */
import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;

public class callTimePeriodReducer extends Reducer< Text,Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double timeofPeriods[] = new double[8];
        double raw_dur_sum = 0.0;

        Iterator iterator = values.iterator();

        while (iterator.hasNext()) {

            String mixdata0 = iterator.next().toString();
            String mixdata[] = mixdata0.split(",");
            String start_time = mixdata[0];
            String end_time = mixdata[1];
            double raw_dur = Integer.parseInt(mixdata[2]);

            raw_dur_sum = raw_dur_sum + raw_dur;

            int start_hour = Integer.parseInt(start_time.split(":")[0]);
            int start_minute = Integer.parseInt(start_time.split(":")[1]);
            int start_second = Integer.parseInt(start_time.split(":")[2]);

            int end_hour = Integer.parseInt(end_time.split(":")[0]);
            int end_minute = Integer.parseInt(end_time.split(":")[1]);
            int end_second = Integer.parseInt(end_time.split(":")[2]);

            int startHourMod = (int)Math.floor(start_hour / 3);
            int endHourMod = (int)Math.floor(end_hour / 3);

            if (startHourMod == endHourMod){

                // 起始时间在同一个时间区段内，则把这段时间全部计入(加入)该段的通话时间(单位为s)
                timeofPeriods[startHourMod] += ((end_hour- start_hour)*60*60 + (end_minute - start_minute) * 60 + end_second - start_second);

            }else if (startHourMod < endHourMod){

                // 在同一天内，结束时间和开始时间不在同一个时间区段里，分段存(加)时间(单位为s)
                timeofPeriods[startHourMod] += ((startHourMod + 1)*3*60*60 - start_hour*60*60 -
                        start_minute*60 - start_second);
                if (endHourMod - startHourMod > 1){
                    // 起码跨了一个完整的时间段
                    for(int j = startHourMod + 1; j < endHourMod; j++){
                        timeofPeriods[j] += 3*60*60;
                    }
                }
                timeofPeriods[endHourMod] += (end_hour*60*60 + end_minute*60 + end_second - endHourMod*3*60*60);

            }else if (startHourMod > endHourMod){

                //跨越了24点，开始于24点前，结束在24点后
                timeofPeriods[startHourMod] += ((startHourMod + 1)*3*60*60 - start_hour*60*60 - start_minute*60 - start_second);
                if (startHourMod < 7){
                    // 起码跨了一个完整的时间段
                    for (int k = startHourMod + 1; k <= 7; k++){
                        timeofPeriods[k] += 3*60*60;
                    }
                }
                for (int i = 0; i < endHourMod; i++){
                    timeofPeriods[i] += 3*60*60;  //一个时间段占时间10800秒
                }
                timeofPeriods[endHourMod] += (end_hour*60*60 + end_minute*60 + end_second - endHourMod*3*60*60);
            }

        }

        String timeRatio[] = new String[8];
        for(int i = 0 ; i<=7; i++){
            timeRatio[i] = String.valueOf(timeofPeriods[i] / raw_dur_sum);
        }

        context.write(key, new Text(timeRatio[0] + ',' + timeRatio[1] + ',' + timeRatio[2] + ',' +
                timeRatio[3] + ',' + timeRatio[4] + ',' + timeRatio[5] + ',' + timeRatio[6] + ',' +
                timeRatio[7]));
    }
}

