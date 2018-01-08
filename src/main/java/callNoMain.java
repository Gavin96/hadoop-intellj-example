/**
 * Created by Administrator on 2018/1/1.
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.StringTokenizer;

public class callNoMain {

    public static void main(String[] args) throws Exception {

        UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser("root");

        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {

                Configuration conf = new Configuration();
                String[] otherArgs = new String[]{"input/tb_call_201202_pro1_easysample.txt","output_callNo"}; //test
//                String[] otherArgs = new String[]{"input/source/tb_call_201202_random.txt","output_callNo"};

                if(otherArgs.length != 2){
                    System.err.println("Usage: callNoMain <input path> <output path>");
                    System.exit(-1);
                }

                FileUtil.deleteDir(conf, otherArgs[1]);

//                Job job = new Job();
                Job job = Job.getInstance(conf, "callNoMain");
                job.setJarByClass(callNoMain.class);
                job.setJobName("get dailyAvg call_Num");

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

                job.setMapperClass(callNoMapper.class);
                job.setReducerClass(callNoReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                System.exit(job.waitForCompletion(true) ? 0 : 1);

                return null;
            }
        });
    }
}
