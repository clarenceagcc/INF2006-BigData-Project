import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.io.IOException;

public class ReviewMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject review = new JSONObject(value.toString());

            // Extract required fields
            String reviewId = review.optString("review_id", "").trim();
            String userId = review.optString("user_id", "").trim();
            String businessId = review.optString("business_id", "").trim();
            int stars = review.optInt("stars", -1);
            int useful = review.optInt("useful", 0);
            int funny = review.optInt("funny", 0);
            int cool = review.optInt("cool", 0);
            String text = review.optString("text", "").trim();
            String rawDate = review.optString("date", "").trim();
            String formattedDate = "00-00-0000";

            // Clean the text field: replace newlines and tabs with spaces
            text = text.replaceAll("[\\t\\n\\r]+", " ")
                      .replaceAll("\\s+", " ")  // Collapse multiple spaces
                      .trim();

            // Format date from yyyy-MM-dd to dd-MM-yyyy
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");
                Date date = inputFormat.parse(rawDate.split(" ")[0]); // Only take date part
                formattedDate = outputFormat.format(date);
            } catch (ParseException e) {
                // Keep default value if parsing fails
            }

            // Drop records with missing essential fields
            if (reviewId.isEmpty() || userId.isEmpty() || businessId.isEmpty() || 
                rawDate.isEmpty() || text.isEmpty() || stars == -1) {
                return;
            }

            // Output format: review_id\tuser_id\tbusiness_id\tstars\tuseful\tfunny\tcool\ttext\tdate
            String cleanedData = String.format("%s\t%s\t%d\t%d\t%d\t%d\t%s\t%s",
                    userId, businessId, stars, useful, funny, cool, text, formattedDate);

            context.write(new Text(reviewId), new Text(cleanedData));

        } catch (Exception e) {
            // Skip malformed JSON entries
        }
    }
}