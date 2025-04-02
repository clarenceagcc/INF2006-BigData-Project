package yelpuserdataset;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.io.IOException;

public class UserMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject user = new JSONObject(value.toString());

            // Extracting fields
            String userId = user.optString("user_id", "UNKNOWN");
            String name = user.optString("name", "UNKNOWN");
            int reviewCount = user.optInt("review_count", 0);
            double avgStars = user.optDouble("average_stars", 0.0);
            String rawDate = user.optString("yelping_since", "0000-00");
            String yelpingSinceFormatted = "00-00-0000";

            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM");
                SimpleDateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");
                Date date = inputFormat.parse(rawDate);
                yelpingSinceFormatted = outputFormat.format(date);
            } catch (ParseException e) {
                // Keep default value if parsing fails
            }

            // Extract useful, funny, cool
            int useful = user.optInt("useful", 0);
            int funny = user.optInt("funny", 0);
            int cool = user.optInt("cool", 0);

            // Extract elite years count (handle both string and array)
            int eliteYears = 0;
            Object eliteObj = user.opt("elite");
            if (eliteObj instanceof JSONArray) {
                eliteYears = ((JSONArray) eliteObj).length();
            } else if (eliteObj instanceof String) {
                String eliteString = (String) eliteObj;
                if (!eliteString.isEmpty()) {
                    eliteYears = eliteString.split(",").length;
                }
            }

            // Extract number of friends (handle both string and array)
            int friendCount = 0;
            Object friendsObj = user.opt("friends");
            if (friendsObj instanceof JSONArray) {
                friendCount = ((JSONArray) friendsObj).length();
            } else if (friendsObj instanceof String) {
                String friendsString = (String) friendsObj;
                if (!friendsString.isEmpty()) {
                    friendCount = friendsString.split(",").length;
                }
            }

            // Extract fans count
            int fans = user.optInt("fans", 0);

            // Extract compliments (sum them)
            int complimentHot = user.optInt("compliment_hot", 0);
            int complimentCool = user.optInt("compliment_cool", 0);
            int complimentFunny = user.optInt("compliment_funny", 0);
            int complimentMore = user.optInt("compliment_more", 0);
            int complimentWriter = user.optInt("compliment_writer", 0);
            int complimentPlain = user.optInt("compliment_plain", 0);
            int complimentPhotos = user.optInt("compliment_photos", 0);
            int complimentNote = user.optInt("compliment_note", 0);
            int complimentProfile = user.optInt("compliment_profile", 0);
            int complimentList = user.optInt("compliment_list", 0);
            int complimentCute = user.optInt("compliment_cute", 0);

            int totalCompliments = complimentHot + complimentCool + complimentFunny + complimentMore +
                    complimentWriter + complimentPlain + complimentPhotos + complimentNote +
                    complimentProfile + complimentList + complimentCute;

            // Output format: user_id \t name \t review_count \t avg_stars \t yelping_since \t useful \t funny \t cool \t
            // elite_years \t friend_count \t fans \t compliment_hot \t compliment_cool \t compliment_funny \t ... totalCompliments
            String cleanedData = String.format("%s\t%d\t%.2f\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
                    name, reviewCount, avgStars, yelpingSinceFormatted, useful, funny, cool, eliteYears, friendCount, fans,
                    complimentHot, complimentCool, complimentFunny, complimentMore, complimentWriter,
                    complimentPlain, complimentPhotos, complimentNote, complimentProfile, totalCompliments);

            context.write(new Text(userId), new Text(cleanedData));

        } catch (Exception e) {
            // Skip malformed JSON entries
        }
    }
}