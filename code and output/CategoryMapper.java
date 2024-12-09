import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CategoryMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable views = new IntWritable();
    private final Text categoryId = new Text();
    private boolean isHeader = true; // Flag to track the header row

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (isHeader) {
            isHeader = false;
            return;
        }
// Split the CSV line
        String[] fields = value.toString().split(",");
        try {
            // Ensure the CSV has the expected number of columns
            if (fields.length > 8) {
                categoryId.set(fields[5]);  // category_id is the 6th column (index 5)
                views.set(Integer.parseInt(fields[8]));  // views is the 9th column (index 8)
                context.write(categoryId, views);
            }
} catch (NumberFormatException e) {
            // Log or handle malformed rows, but continue processing
            System.err.println("Skipping malformed row: " + value.toString());
        }
    }
}
