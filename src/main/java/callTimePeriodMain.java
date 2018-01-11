/**
 * Created by Administrator on 2018/1/3.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.security.PrivilegedExceptionAction;

public class callTimePeriodMain {

    public static void main(String[] args) throws Exception {

        UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser("root");

        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {

                Configuration conf = new Configuration();
//                String[] otherArgs = new String[]{"input/tb_call_201202_pro3_easysample.txt","output_calledTimeRatio"}; //test
                String[] otherArgs = new String[]{"input/source/tb_call_201202_random.txt","output_calledTimeRatio"};
//                String[] otherArgs = new String[]{"input/tb_call_201202_pro3_easysample2.txt","output_calledTimeRatio"};
//                String[] otherArgs = new String[]{"input/tb_call_201202_pro3_easysample3.txt","output_calledTimeRatio"};

                if(otherArgs.length != 2){
                    System.err.println("Usage: callTimePeriodMain <input path> <output path>");
                    System.exit(-1);
                }

                FileUtil.deleteDir(conf, otherArgs[1]);

//                Job job = new Job();
                Job job = Job.getInstance(conf, "callTimePeriodMain");
                job.setJarByClass(callTimePeriodMain.class);
                job.setJobName("get callTimeRatio of each calling_nbr");

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

                job.setMapperClass(callTimePeriodMapper.class);
                job.setReducerClass(callTimePeriodReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                System.exit(job.waitForCompletion(true) ? 0 : 1);

                return null;
            }
        });
    }
}
