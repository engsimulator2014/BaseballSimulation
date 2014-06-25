package com.simulation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Description: Take player stats from Mapper and reduces them to career stats
 * Input:  [playerId] => Iterable<[G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP]>
 * Output: [playerId] => Career Stats (sum of each batting category)
 *
 */
public class PlayerBattingReducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * Reduce yearly player stats to career stats
	 * @param key
	 * @param values
	 * @param context
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

		String careerBattingStats = null;
		String battingData = null;
		for (Text line : values) {
			battingData = line.toString();
			careerBattingStats = sumStats(careerBattingStats, battingData);
		}
		context.write(key, new Text(careerBattingStats));
	}

	/**
	 * Sum yearly stats to create career stats
	 * battingLine: G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP
	 * @param careerBattingStats
	 * @param battingLine
	 * @return sum of careerBattingStats in CSV format
	 */
	private String sumStats(String careerBattingStats, String battingLine) {
		String[] fields = null;
		Integer[] careerStats = null;
		if(careerBattingStats == null) {
			careerStats = new Integer[] { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };
		} else {
			fields = careerBattingStats.split(",");
			careerStats = new Integer[fields.length];
			for(int i = 0; i < fields.length; i++) {
				careerStats[i] = Integer.parseInt(fields[i]);
			}
		}
		fields = battingLine.split(",");
		Integer[] stats = new Integer[fields.length];
		for(int i = 0; i < fields.length; i++) {
			stats[i] = Integer.parseInt(fields[i]);
			careerStats[i] = careerStats[i] + stats[i];
		}

		return toCSVString(careerStats);
	}

	/**
	 * Turn an array of Integers into a CSV string
	 * @param fields
	 * @return CSV string
	 */
	private static String toCSVString(Integer[] fields) {
		if(fields == null) {
			return null;
		}
		int n = fields.length - 1;
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < fields.length; i++) {
			sb.append(fields[i]);
			if(i < n) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
}