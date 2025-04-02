package yelpuserdataset;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

public class UserMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject obj = new JSONObject(value.toString());

            if (obj.has("user_id") && obj.has("text") && obj.has("compliment_count")) {
                // Tip Data
                String userId = obj.optString("user_id", "UNKNOWN");
                String businessId = obj.optString("business_id", "UNKNOWN");
                String text = obj.optString("text", "").replaceAll("\t", " ").replaceAll("\n", " ");
                String date = obj.optString("date", "");
                int complimentCount = obj.optInt("compliment_count", 0);

                String tipData = String.format("TIP\t%s\t%s\t%s\t%d", userId, text, date, complimentCount);
                context.write(new Text(businessId), new Text(tipData));

            } else if (obj.has("date") && obj.has("business_id") && obj.length() == 2) {
                // Check-in Data
                String businessId = obj.optString("business_id", "UNKNOWN");
                String checkinDates = obj.optString("date", "").replaceAll("\t", " ").replaceAll("\n", " ");

                String checkinData = "CHECKIN\t" + checkinDates;
                context.write(new Text(businessId), new Text(checkinData));

            } else if (obj.has("name") && obj.has("address") && obj.has("city")) {
                // Business Data
                String businessId = obj.optString("business_id", "UNKNOWN");
                String name = obj.optString("name", "").replaceAll("\t|\n", " ");
                String address = obj.optString("address", "").replaceAll("\t|\n", " ");
                String city = obj.optString("city", "");
                String state = obj.optString("state", "");
                String postalCode = obj.optString("postal_code", "");
                double latitude = obj.optDouble("latitude", 0.0);
                double longitude = obj.optDouble("longitude", 0.0);
                double stars = obj.optDouble("stars", 0.0);
                int reviewCount = obj.optInt("review_count", 0);
                int isOpen = obj.optInt("is_open", -1);
                String categories = obj.optString("categories", "").replaceAll("\t|\n", " ");

                String businessData = String.format(
                    "BUSINESS\t%s\t%s\t%s\t%s\t%s\t%.6f\t%.6f\t%.1f\t%d\t%d\t%s",
                    name, address, city, state, postalCode,
                    latitude, longitude, stars, reviewCount, isOpen, categories
                );

                context.write(new Text(businessId), new Text(businessData));
            }
        } catch (Exception e) {
            // Skip malformed or unrecognized JSON entries
        }
    }
}