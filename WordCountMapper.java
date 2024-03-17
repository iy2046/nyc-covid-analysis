import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text year = new Text();
    private IntWritable avgDeathCount = new IntWritable();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length > 8) {
            String date = fields[0].substring(1, 5);
            year.set(date);
            avgDeathCount.set( Integer.parseInt( fields[8].replaceAll("\"", "") ) );
            context.write(year, avgDeathCount);
        }
    }
}