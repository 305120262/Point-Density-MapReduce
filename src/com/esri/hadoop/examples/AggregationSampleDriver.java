package com.esri.hadoop.examples;

import java.io.Console;
import java.io.FileInputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.OperatorBuffer;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.json.EsriFeatureClass;


public class AggregationSampleDriver
{
	public static int main(String[] init_args) throws Exception {	
		
		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();
		
		
		// Args
		//  [0] path to Esri JSON file
		//  [1] path(s) to the input data source
		//  [2] path to write the output of the MapReduce jobs
		 
		if (args.length != 7)
		{
			System.out.println("Invalid Arguments");
			print_usage();
			throw new IllegalArgumentException();
		}
		
		config.set("sample.features.input", args[0]);
		config.set("sample.features.source", args[1]);
		config.set("sample.features.output", args[2]);
		config.setFloat("radius", Float.parseFloat(args[3]));
		config.set("sample.features.keyattribute", args[4]);
		config.setInt("samples.csvdata.columns.lat", Integer.parseInt(args[5]));
		config.setInt("samples.csvdata.columns.long", Integer.parseInt(args[6]));
		
		
		return execute(config);
	}
	


	static private int execute(Configuration config)throws Exception 
	{
		Job job = new Job(config);

		job.setJobName("Point Density Sample");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		// In our case, the combiner is the same as the reducer.  This is possible
		// for reducers that are both commutative and associative 
		job.setCombinerClass(ReducerClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(config.get("sample.features.source")));
		TextOutputFormat.setOutputPath(job, new Path(config.get("sample.features.output")));

		job.setJarByClass(AggregationSampleDriver.class);

		return job.waitForCompletion(true)?  0 : 1;
	}
	
	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar aggregation-sample.jar AggregationSampleDriver -libjars [external jar references] [/hdfs/path/to]/testgrid.json [/hdfs/path/to]/earthquakes.csv [/hdfs/path/to/user]/output.output [radius] [GridID] [LatFieldIndex] [LongFieldIndex]");
		System.out.println("***");
	}
	
	/*public static void main(String[] init_args) throws Exception {
		test();
	}*/
	static private void test() throws Exception
	{
		SpatialReference spatialReference = SpatialReference.create(4326);

		FileInputStream iStream = new FileInputStream("E:\\工作任务\\20130327技术创新研究\\Data\\testgrid.json");
		EsriFeatureClass featureClass = EsriFeatureClass.fromJson(iStream);
		
		//建立四叉树索引
		QuadTree quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), 8);
		Envelope envelope = new Envelope();
		for (int i=0;i<featureClass.features.length;i++){
			featureClass.features[i].geometry.queryEnvelope(envelope);
			quadTree.insert(i, new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
		}		
		
		Point point = new Point(114, 13);
		ArrayList<Integer> result = new ArrayList<Integer>();
		Geometry buffer = OperatorBuffer.local().execute(point,spatialReference, 10, null);
		
/*		
 * 		循环判断方法，效率低
 * 		for (int i=0;i<featureClass.features.length;i++){
			if (GeometryEngine.disjoint(featureClass.features[i].geometry, buffer, spatialReference)==false){
				result.add(i);
			}
		}
		for(int i : result){
			System.out.printf("%d%n",i);
		}*/
		
		//采用四叉树索引，效率高
		buffer.queryEnvelope(envelope);
		QuadTreeIterator quadTreeIter  = quadTree.getIterator(new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()), 0); 
		
		int elmHandle = quadTreeIter.next();
		while (elmHandle >= 0){
			int featureIndex = quadTree.getElement(elmHandle);
			if(result.contains(featureIndex)==false){
				if (GeometryEngine.intersect(featureClass.features[featureIndex].geometry, buffer, spatialReference)!=null){
					result.add(featureIndex);
				}
			}
			elmHandle = quadTreeIter.next();
		}
		for(int i : result){
			System.out.printf("%d%n",i);
		}
		
	}
	

}
