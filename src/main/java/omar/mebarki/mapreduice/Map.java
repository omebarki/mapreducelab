package omar.mebarki.mapreduice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        StringTokenizer wordsTokenizer = new StringTokenizer(line.toString());
        while (wordsTokenizer.hasMoreTokens()) {
            context.write(new Text(wordsTokenizer.nextToken()), new IntWritable(1));
        }
    }
}
