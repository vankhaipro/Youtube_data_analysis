import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();
    private Map<String, String> categoryMap = new HashMap<>();

@Override
    protected void setup(Context context) throws IOException {
        // Load category JSON from file
        String jsonFilePath = context.getConfiguration().get("category.json.path");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonFilePath));
        for (JsonNode item : root.get("items")) {
 String id = item.get("id").asText();
            String title = item.get("snippet").get("title").asText();
            categoryMap.put(id, title);
        }
    }

@Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

    // Replace getOrDefault with compatible logic
        String categoryTitle = categoryMap.containsKey(key.toString())
                ? categoryMap.get(key.toString())
                : "Unknown";

        context.write(new Text(categoryTitle), new IntWritable(sum));
    }
}

