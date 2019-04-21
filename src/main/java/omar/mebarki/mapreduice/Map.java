package omar.mebarki.mapreduice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        StringTokenizer wordsTokenizer = new StringTokenizer(line.toString());
        java.util.Map<String, Integer> wordsCount = new HashMap<String, Integer>();
        while (wordsTokenizer.hasMoreTokens()) {
            String word = wordsTokenizer.nextToken();
            int initial = wordsCount.getOrDefault(word, 0);
            wordsCount.put(word, ++initial);
        }
        for (java.util.Map.Entry<String, Integer> wordEntry :
                wordsCount.entrySet()) {
            context.write(new Text(wordEntry.getKey()), new IntWritable(wordEntry.getValue()));
        }
    }
}
