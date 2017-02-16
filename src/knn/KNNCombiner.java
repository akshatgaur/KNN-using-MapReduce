package knn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class KNNCombiner extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value, Context context) throws java.io.IOException, InterruptedException {

		ArrayList<DistanceLabelPair> vs = new ArrayList<DistanceLabelPair>();

		for (Text v : value) {
			DistanceLabelPair obj = new DistanceLabelPair();
			String[] s = v.toString().split(",");
			obj.distance = Float.parseFloat(s[0]);
			obj.label = s[1];
			vs.add(obj);
		}

		Collections.sort(vs, new Comparator<DistanceLabelPair>() {
			@Override
			public int compare(DistanceLabelPair o1, DistanceLabelPair o2) {
				if (o1.distance > o2.distance){
					return 1;
				}
				return -1;
			}
		});

		int k = context.getConfiguration().getInt("org.niubility.knn.k", 5);
		HashMap<String, Integer> res = new HashMap<String, Integer>();
		for (int i = 0; i < k && i < vs.size(); i++) {

			if (!res.containsKey(vs.get(i).label)){
				res.put(vs.get(i).label, 0);
			}
			res.put(vs.get(i).label, res.get(vs.get(i).label) + 1);
		}
		int max = 0;
		String label = "";
		for(String l : res.keySet()){
			if(max < res.get(l)){
				max = res.get(l);
				label = l;
			}
		}
		context.write(key, new Text(label));
	}

}