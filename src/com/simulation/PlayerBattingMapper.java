package com.simulation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Description: PlayerBattingMapper reads lahman2012/Batting.csv and emits playerId as key
 * and yearly batting stats for each year a player played, PlayerBattingTotalsReducer will reduce
 * to career stat line for every MLB player.
 *
 * Batting.csv, 24 fields, blank fields are emitted as 0, not all fields are emitted
 *
 * Header and example input line:
 * playerID,yearID,stint,teamID,lgID,G,G_batting,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,G_old
 * aaronha01,1954,1,ML1,NL,122,122,468,58,131,27,6,13,69,2,2,28,39,,3,6,4,13,122
 *
 * Emits: [playerId] => [G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP]
 * Legend:
 * G=games,AB=at bats,R=runs,H=hits,2B=doubles,3B=triples,HR=dinger,RBI=runs batted in,
 * SB=stolen base,CS=caught stealing,BB=base on balls,SO=strikeout,IBB=intentional walks,
 * HBP=hit by pitch,SH=sacrifice hits,SF=sacrifice flys,GIDP=ground into double play
 *
 */
public class PlayerBattingMapper extends Mapper<LongWritable, Text, Text, Text> {
	/*
	 * number of fields in batting file
	 */
	private static final int FIELD_COUNT = 24;
	/*
	 * Map field names to index position (0 based)
	 */
	private static final Map<String,Integer> fields;
	static {
		fields = new HashMap<String,Integer>();
		fields.put("G", 5);
		fields.put("AB", 7);
		fields.put("R", 8);
		fields.put("H", 9);
		fields.put("2B", 10);
		fields.put("3B", 11);
		fields.put("HR", 12);
		fields.put("RBI", 13);
		fields.put("SB", 14);
		fields.put("CS", 15);
		fields.put("BB", 16);
		fields.put("SO", 17);
		fields.put("IBB", 18);
		fields.put("HBP", 19);
		fields.put("SH", 20);
		fields.put("SF", 21);
		fields.put("GIDP", 22);
	}

	/*
	 * The map method runs once for each line of text in the input file. The method receives a key
	 * of type LongWritable, a value of type Text, and a Context object.
	*/
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		/*
		 * skip header
		 */
		long lineNum = key.get();
		if(lineNum <= 1)
			return;

		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String line = value.toString();

		/*
		 * The line.split(",") splits the line on commas
		 */
		String battingData[] = line.split(",");
		// G_old (last field) isn't always present in data
		if(battingData.length < (FIELD_COUNT-1)) { 
			System.out.println("Line: " + lineNum
				+ " contains bad data! Should have at least"
				+ (FIELD_COUNT-1) + " fields but only found "
				+ battingData.length);
			return;
		}

		// G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP
		StringBuffer sb = new StringBuffer();
		sb.append(getBattingData("G", battingData)).append(",");
		sb.append(getBattingData("AB", battingData)).append(",");
		sb.append(getBattingData("R", battingData)).append(",");
		sb.append(getBattingData("H", battingData)).append(",");
		sb.append(getBattingData("2B", battingData)).append(",");
		sb.append(getBattingData("3B", battingData)).append(",");
		sb.append(getBattingData("HR", battingData)).append(",");
		sb.append(getBattingData("RBI", battingData)).append(",");
		sb.append(getBattingData("SB", battingData)).append(",");
		sb.append(getBattingData("CS", battingData)).append(",");
		sb.append(getBattingData("BB", battingData)).append(",");
		sb.append(getBattingData("SO", battingData)).append(",");
		sb.append(getBattingData("IBB", battingData)).append(",");
		sb.append(getBattingData("HBP", battingData)).append(",");
		sb.append(getBattingData("SH", battingData)).append(",");
		sb.append(getBattingData("SF", battingData)).append(",");
		sb.append(getBattingData("GIDP", battingData));

		context.write(new Text(battingData[0]), new Text(sb.toString()));

	}

	/**
	 * Get batting stats from battingData array, empty string '' or null resolve to 0
	 * @param fieldName
	 * @param battingData
	 * @return batting stat value as a String
	 */
	private String getBattingData(String fieldName, String[] battingData) {

		String value = battingData[fields.get(fieldName)];
	    if(value == null || "".equals(value)) {
	    	value = "0";
	    }
	    return value;
	}
}
