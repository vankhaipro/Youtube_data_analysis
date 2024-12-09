import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class CategoryMapperUtility {
    public static Map<String, String> parseCategoryJSON(String jsonFilePath) throws Exception {
        Map<String, String> categoryMap = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonFilePath));
        JsonNode items = root.get("items");
        for (JsonNode item : items) {
            String id = item.get("id").asText();
            String title = item.get("snippet").get("title").asText();
            categoryMap.put(id, title);
        }
        return categoryMap;
    }
}
