package de.tu_berlin.impro3.flink.model;

import com.google.gson.Gson;
import de.tu_berlin.impro3.flink.io.GsonTweetInputFormat;
import de.tu_berlin.impro3.flink.model.tweet.Tweet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Created by mustafa on 10/17/14.
 */
public class TestGSON {

    public static void main(String[] args) {

        Tweet tweet = new Tweet();
        GsonTweetInputFormat gsonTweetInputFormat = new GsonTweetInputFormat();
        Gson gson = new Gson();

        try {

            Path path = Paths.get("/home/mustafa/Downloads/Twiter Api/Test.json");
            byte[] data = Files.readAllBytes(path);
            tweet = gsonTweetInputFormat.readRecord(tweet, data, 0, 0);
            //    tweet = gson.fromJson(new InputStreamReader(new ByteArrayInputStream(data)), Tweet.class);
            System.out.println(tweet.getText());


        } catch (IOException e) {

        }

    }
}
