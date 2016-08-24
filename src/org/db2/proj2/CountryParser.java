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

public class CountryParser {
	String line = "";
	String countryFile = "C:\\UTA\\Summer2016\\Project2\\Input files\\Country.csv";
	String playerAssistsGoalsFile = "C:\\UTA\\Summer2016\\Project2\\Input files\\Player_Assists_Goals.csv";
	String playerCardsFile = "C:\\UTA\\Summer2016\\Project2\\Input files\\Player_Cards.csv";
	String playersFile = "C:\\UTA\\Summer2016\\Project2\\Input files\\Players.csv";
	String wcHistoryFile = "C:\\UTA\\Summer2016\\Project2\\Input files\\Worldcup_History.csv";

	public JSONObject countryParse() throws JSONException {
		JSONObject jsonObject = new JSONObject();
		try {
			BufferedReader br = new BufferedReader(new FileReader(countryFile));

			while ((line = br.readLine()) != null) {
				JSONObject countryJson = new JSONObject();

				String[] countries = line.split(",");
				countryJson.put(CountryColumns.COUNTRY_NAME, countries[CountryColumns.COUNTRY_NAME_INDEX].replace("'", ""));
				countryJson.put(CountryColumns.COUNTRY_MANAGER, countries[CountryColumns.COUNTRY_MANAGER_INDEX].replace("'", ""));
				countryJson.put(CountryColumns.COUNTRY_POPULATION, Double.parseDouble(countries[CountryColumns.COUNTRY_POPULATION_INDEX].replace("'", "")));
				countryJson.put(CountryColumns.PLAYER_CAPS_FOR_COUNTRY, countries[CountryColumns.PLAYER_CAPS_FOR_COUNTRY_INDEX].replace("'", ""));

				jsonObject.put(countries[CountryColumns.COUNTRY_NAME_INDEX].replace("'", ""),countryJson);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return jsonObject;

	}

	public JSONObject playersParse() throws JSONException {
		JSONObject jsonObject = new JSONObject();
		try {
			BufferedReader br = new BufferedReader(new FileReader(playersFile));


			while ((line = br.readLine()) != null) {
				JSONObject playerJson = new JSONObject();

				String[] players = line.split(",");
				playerJson.put(CountryColumns.PLAYER_DOB, players[CountryColumns.PLAYER_DOB_INDEX].replace("'", ""));
				playerJson.put(CountryColumns.PLAYER_FIRST_NAME, players[CountryColumns.PLAYER_FIRST_NAME_INDEX].replace("'", ""));
				playerJson.put(CountryColumns.PLAYER_LAST_NAME, players[CountryColumns.PLAYER_LAST_NAME_INDEX].replace("'", ""));
				playerJson.put(CountryColumns.PLAYER_HEIGHT, Integer.parseInt(players[CountryColumns.PLAYER_HEIGHT_INDEX]));
				playerJson.put(CountryColumns.PLAYER_POSITION, players[CountryColumns.PLAYER_POSITION_INDEX].replace("'", ""));
				playerJson.put(CountryColumns.PLAYER_IS_CAPTAIN, players[CountryColumns.PLAYER_IS_CAPTAIN_INDEX].replace("'", ""));

				jsonObject.put(players[CountryColumns.PLAYER_ID_INDEX]+'$'+players[CountryColumns.PLAYER_COUNTRY_INDEX].replace("'", ""),playerJson);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return jsonObject;
	}
	public JSONObject playersAssistsParse() throws JSONException {
		JSONObject jsonObject = new JSONObject();
		try {
			BufferedReader br = new BufferedReader(new FileReader(playerAssistsGoalsFile));


			while ((line = br.readLine()) != null) {
				JSONObject playersAssistJSON = new JSONObject();

				String[] playerAssists = line.split(",");
				playersAssistJSON.put(CountryColumns.PLAYER_ASSIST_ASSISTS, playerAssists[CountryColumns.PLAYER_ASSIST_ASSISTS_INDEX]);
				playersAssistJSON.put(CountryColumns.PLAYER_ASSIST_GOALS, playerAssists[CountryColumns.PLAYER_ASSIST_GOALS_INDEX]);

				jsonObject.put(playerAssists[CountryColumns.PLAYER_ID_INDEX],playersAssistJSON);
			}
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return jsonObject;
	}

	public JSONObject playerCardsParse() throws JSONException {
		JSONObject jsonObject = new JSONObject();
		try {
			BufferedReader br = new BufferedReader(new FileReader(playerCardsFile));


			while ((line = br.readLine()) != null) {
				JSONObject playerCardJson = new JSONObject();

				String[] playerCards = line.split(",");
				playerCardJson.put(CountryColumns.PLAYER_CARD_NO_OF_RED_CARDS, playerCards[CountryColumns.PLAYER_CARD_NO_OF_RED_CARDS_INDEX]);
				playerCardJson.put(CountryColumns.PLAYER_CARD_NO_OF_YELLOW_CARDS, playerCards[CountryColumns.PLAYER_CARD_NO_OF_YELLOW_CARDS_INDEX]);

				jsonObject.put(playerCards[CountryColumns.PLAYER_ID_INDEX],playerCardJson);
			}
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return jsonObject;
	}

	/**
	 * It parses the history CSV and return JSONObject 
	 * @return JSONObject
	 * {
	 * 		country:[
		 * 		{
		 * 			Year:..,
		 * 			Host:..
		 * 		},
		 * 		{
		 * 			Year:..,
		 * 			Host:..
		 * 		}
	 * 		]
	 * } 
	 * @throws JSONException
	 */
	public JSONObject historyParse() throws JSONException {
		JSONObject jsonObject = new JSONObject();
		try {
			BufferedReader br = new BufferedReader(new FileReader(wcHistoryFile));


			while ((line = br.readLine()) != null) {
				JSONObject historyJson = new JSONObject();
				JSONArray historyOfaCountry= new JSONArray(); 

				String[] country = line.split(",");
				historyJson.put(CountryColumns.WC_HIST_HOST, country[CountryColumns.WC_HIST_HOST_INDEX].replace("'", ""));
				try{
					historyJson.put(CountryColumns.WC_HIST_YEAR, Integer.parseInt(country[CountryColumns.WC_HIST_YEAR_INDEX]));
				}
				catch(NumberFormatException e){
					
				}
				if(jsonObject.has(country[CountryColumns.WC_HIST_WINNER_INDEX].replace("'", ""))){
					historyOfaCountry = (JSONArray) jsonObject.get(country[CountryColumns.WC_HIST_WINNER_INDEX].replace("'", ""));
					historyOfaCountry.put(historyJson);
					jsonObject.put(country[CountryColumns.WC_HIST_WINNER_INDEX].replace("'", ""),historyOfaCountry);
				}else{
					historyOfaCountry.put(historyJson);
					jsonObject.put(country[CountryColumns.WC_HIST_WINNER_INDEX].replace("'", ""),historyOfaCountry);	            	
				}

			}
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return jsonObject;
	}

	public JSONArray countryPlayerCombine() throws JSONException {
		JSONArray resJsonArray = new JSONArray();
		JSONObject countryJSON = countryParse();
		JSONObject playerJSON = playerFileParse();
		JSONObject historyJSON = historyParse();
		Iterator<?> keys = countryJSON.keys();
		while(keys.hasNext()) {
			String country = (String) keys.next();
			JSONObject jsonObject = countryJSON.getJSONObject(country);
			try{
				JSONArray playerJsonObject = (JSONArray) playerJSON.get(country);
				jsonObject.put(CountryColumns.PLAYERS,playerJsonObject);
			}
			catch (Exception e){
			}
			try{
				JSONArray pAJsonObject = (JSONArray) historyJSON.get(country);
				jsonObject.put(CountryColumns.HISTORY,pAJsonObject);
			}
			catch (Exception e){
			}
			resJsonArray.put(jsonObject);
		}
		return resJsonArray;
	}

	public JSONObject playerFileParse() throws JSONException {
		JSONObject resJsonObject = new JSONObject();
		JSONObject playerJSON = playersParse();
		JSONObject playerCardJSON = playerCardsParse();
		JSONObject playerAssistsJSON = playersAssistsParse();
		Iterator<?> keys = playerJSON.keys();
		while(keys.hasNext()) {
			String j = (String) keys.next();
			String playerId = j.split("\\$")[0];
			String countryHost = j.split("\\$")[1];
			JSONObject jsonObject = playerJSON.getJSONObject(j);
			try{
				JSONObject pCJsonObject = (JSONObject) playerCardJSON.get(playerId);
				Iterator<?> pCKeys = pCJsonObject.keys();
				while(pCKeys.hasNext()) {
					String pC = (String) pCKeys.next();
					int value = pCJsonObject.getInt(pC);
					jsonObject.put(pC, value);
				}
			}
			catch (Exception e){

			}
			try{
				JSONObject pAJsonObject = (JSONObject) playerAssistsJSON.get(playerId);
				Iterator<?> pAKeys = pAJsonObject.keys();
				while(pAKeys.hasNext()) {
					String pA = (String) pAKeys.next();
					int value = pAJsonObject.getInt(pA);
					jsonObject.put(pA, value);
				}
			}
			catch (Exception e){

			}
			JSONArray j1 = new JSONArray();
			if(resJsonObject.has(countryHost)){
				j1 = (JSONArray) resJsonObject.get(countryHost);
				j1.put(jsonObject);
				resJsonObject.put(countryHost,j1);
			}else{
				j1.put(jsonObject);
				resJsonObject.put(countryHost,j1);	            	
			}
		}
		return resJsonObject;
	}


	public static void main(String[] args) throws JSONException {
		CountryParser c = new CountryParser();
		
//		prepare the data to insert into mongoDB
		List<Document> docsToInsert = new ArrayList<>();
		JSONArray jsonarray = c.countryPlayerCombine();
		for (int i = 0; i < jsonarray.length(); i++) {
			JSONObject jsonobject = (JSONObject)jsonarray.get(i);
			Document doc = Document.parse(jsonobject.toString());
			docsToInsert.add(doc);
		}
//		insert the data to mongoDB
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("projectwo");
		MongoCollection<Document> collection = db.getCollection("COUNTRY");
		collection.insertMany(docsToInsert);
		mongoClient.close();

	}

}
