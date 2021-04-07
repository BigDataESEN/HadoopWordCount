import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String currentLine = line.toString();
        String[] words = currentLine.split(" ");
        for(String word:words){
            System.out.println(word);
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
