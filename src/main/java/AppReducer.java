import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AppReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Iterable<IntWritable>> values, Context context) throws IOException, InterruptedException {
        Iterator iterator = values.iterator();
        int sum = 0;
        while(iterator.hasNext()){
            IntWritable i = (IntWritable)iterator.next();
            sum += i.get();
        }
        System.out.println(key.toString()+" : "+sum);
        context.write(key, new IntWritable(sum));
    }
}
