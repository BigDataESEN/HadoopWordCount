import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // Créer la configuration
        Configuration conf = new Configuration();
        // Créer le job
        Job job = Job.getInstance(conf, "WordCount");
        // Définir la classe principale
        job.setJarByClass(WordCount.class);
        // Définir la classe Mapper
        job.setMapperClass(AppMapper.class);
        // Définir la classe Reducer
        job.setReducerClass(AppReducer.class);
        // Définir le type du clé output
        job.setOutputKeyClass(Text.class);
        // Définir le type de la valeur output
        job.setOutputValueClass(IntWritable.class);
        // Ajouter le fichier à traiter
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Résultat
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
