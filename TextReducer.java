package com;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.io.IOException;


public class TextReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String translations = "";
        DecimalFormat form = new DecimalFormat("0.00");  
		float [] itr = new float[5];
		int count = 0;
        for (Text val : values) {
        	itr [0] = itr [0] + Float.parseFloat(value.toString().split(",")[2]);
            itr [1] = itr [1] + Float.parseFloat(value.toString().split(",")[3]);
            itr [2] = itr [2] + Float.parseFloat(value.toString().split(",")[4]);
            itr [3] = itr [3] + Float.parseFloat(value.toString().split(",")[5]);
            itr [4] = itr [4] + Float.parseFloat(value.toString().split(",")[6]);
            count = count + 1;
        }

			itr [0] = itr [0]/count;
            itr [1] = itr [1]/count;
            itr [2] = itr [2]/count;
            itr [3] = itr [3]/count;
            itr [4] = itr [4]/count;
            
        translations = key.toString + "," +  form.format(itr [0])  + "," +  form.format(itr [1])+ "," +  form.format(itr [2])+ "," +  form.format(itr [3])+ "," +  form.format(itr [4]);
            
        result.set(translations);
        context.write(key, result);
    }
}