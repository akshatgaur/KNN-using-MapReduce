package knn;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

public class KNNMapper extends Mapper<Object, Text, Text, Text> {

    private ArrayList<ArrayList<Float>> test = new ArrayList<ArrayList<Float>> ();

    protected void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
        // calculate the distance for each test sample with the training
        // data
        context.setStatus(key.toString());
        String[] s = value.toString().split(",");
        String label = s[s.length - 1];

        for (int i = 0; i < test.size(); i++) {
            Text val = new Text();
            ArrayList<Float> testcase;
            Text word = new Text();
            testcase = test.get(i);
            double d = 0.0f;
            for (int j = 0; j < testcase.size(); j++) {
                d += (testcase.get(j) - Float.parseFloat(s[j])) * (testcase.get(j) - Float.parseFloat(s[j]));
            }
            d = Math.sqrt(d);
            val.set((Double.toString(d)) + "," + label);
            String str = Integer.toString(i);
            word.set(str);
            context.write(word, val);
        }

    }

    protected void cleanup(org.apache.hadoop.mapreduce.Mapper<Object, Text, Text,Text>.Context context)
            throws java.io.IOException, InterruptedException {
//        test.close();
    }


    protected void setup(org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>.Context context)
            throws java.io.IOException, InterruptedException {
        System.out.print("loading shared comparison vectors...");

        // load the test vectors
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration().get(
                "org.niubility.learning.test", "/Users/akshatgaur/Downloads/Test/iris_test_data.csv")))));
        String line = br.readLine();
        int count = 0;
        while (line != null) {

            //Vector2<String, SparseVector> v = ARFFInputformat.readLine(count, line);
            //test.add(new Vector2<String, SparseVector>(v.getV1(), v.getV2()));
            String[] s = line.split(",");
            ArrayList<Float> testcase = new ArrayList<Float>();
            for (int i = 0; i < s.length; i++){

                testcase.add(Float.parseFloat(s[i]));
            }
            test.add(testcase);
            line = br.readLine();
            count++;
        }
        br.close();
        System.out.println("done.");
    }

    ;
}
