package org.db2.proj2;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class StadiumParser {
	String line = "";
	String matchresultsFile = "C:\\UTA\\Summer2016\\Project2\\Input files\\Match_results.csv";
	public JSONArray matchresultsfileParse() throws JSONException{
		JSONObject jsonRes = new JSONObject();
		try {
			BufferedReader br = new BufferedReader(new FileReader(matchresultsFile));
			while ((line = br.readLine()) != null) {
				JSONObject stadiumJson = new JSONObject();
				String[] stadium = line.split(",");
				stadiumJson.put(StadiumColumns.MATCH_RESULTS_STADIUM, stadium[StadiumColumns.MATCH_RESULTS_STADIUM_INDEX].replace("'", ""));
				stadiumJson.put(StadiumColumns.MATCH_RESULTS_HOST_CITY, stadium[StadiumColumns.MATCH_RESULTS_HOST_CITY_INDEX].replace("'", ""));
				JSONObject matchesJson = new JSONObject();
				matchesJson.put(StadiumColumns.MATCH_RESULTS_TEAM1, stadium[StadiumColumns.MATCH_RESULTS_TEAM1_INDEX].replace("'", ""));
				matchesJson.put(StadiumColumns.MATCH_RESULTS_TEAM2, stadium[StadiumColumns.MATCH_RESULTS_TEAM2_INDEX].replace("'", ""));
				matchesJson.put(StadiumColumns.MATCH_RESULTS_TEAM1_SCORE, Integer.parseInt(stadium[StadiumColumns.MATCH_RESULTS_TEAM1_SCORE_INDEX]));
				matchesJson.put(StadiumColumns.MATCH_RESULTS_TEAM2_SCORE, Integer.parseInt(stadium[StadiumColumns.MATCH_RESULTS_TEAM2_SCORE_INDEX]));
				matchesJson.put(StadiumColumns.MATCH_RESULTS_DATE, stadium[StadiumColumns.MATCH_RESULTS_DATE_INDEX].replace("'", ""));
				if(jsonRes.has(stadium[StadiumColumns.MATCH_RESULTS_STADIUM_INDEX].replace("'", ""))){
					JSONObject stad = jsonRes.getJSONObject(stadium[StadiumColumns.MATCH_RESULTS_STADIUM_INDEX].replace("'", ""));
					JSONArray matches = (JSONArray) stad.get(StadiumColumns.MATCH_RESULTS_MATCHES);
					matches.put(matchesJson);
					stad.put(StadiumColumns.MATCH_RESULTS_MATCHES, matches);
					jsonRes.put(stadium[StadiumColumns.MATCH_RESULTS_STADIUM_INDEX].replace("'", ""), stad);
				}else{
					JSONArray matches = new JSONArray();
					matches.put(matchesJson);
					stadiumJson.put(StadiumColumns.MATCH_RESULTS_MATCHES, matches);
					jsonRes.put(stadium[StadiumColumns.MATCH_RESULTS_STADIUM_INDEX].replace("'", ""), stadiumJson);
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(jsonRes);
		JSONArray resJSONArray = new JSONArray();
		Iterator<?> keys = jsonRes.keys();
		while(keys.hasNext()){
			String key = (String) keys.next();
			System.out.println(key);
			resJSONArray.put(jsonRes.get(key));
		}
		return resJSONArray;
	}


	public static void main(String[] args) throws JSONException {
		StadiumParser s = new StadiumParser();

		// prepare data to insert into mongoDB
		List<Document> docsToInsert = new ArrayList<>();
		JSONArray jsonarray = s.matchresultsfileParse();
		for (int i = 0; i < jsonarray.length(); i++) {
			JSONObject jsonobject = (JSONObject)jsonarray.get(i);
			Document doc = Document.parse(jsonobject.toString());
			docsToInsert.add(doc);
		}

		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("projectwo");
		MongoCollection<Document> collection = db.getCollection("STADIUM");
		collection.insertMany(docsToInsert);
		mongoClient.close();
	}

}
