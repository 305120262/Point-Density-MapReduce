package com.esri.hadoop.examples;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.OperatorBuffer;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeatureClass;


public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// column indices for values in the CSV
	int longitudeIndex;
	int latitudeIndex;
	
	float distance; 

	// the label for the polygon is "NAME"
	String labelAttribute;
	
	EsriFeatureClass featureClass;
	SpatialReference spatialReference;
	QuadTree quadTree;
	
	private void buildQuadTree(){
		quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), 8);
		
		Envelope envelope = new Envelope();
		for (int i=0;i<featureClass.features.length;i++){
			featureClass.features[i].geometry.queryEnvelope(envelope);
			quadTree.insert(i, new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
		}
		
	}
	
	/**
	 * Query the quadtree for the feature containing the given point
	 * 
	 * @param pt point as longitude, latitude
	 * @return index list to features in featureClass 
	 */
	private ArrayList<Integer> queryQuadTree(Point pt, float radius)
	{
		ArrayList<Integer> result = new ArrayList<Integer>();
		Geometry buffer = OperatorBuffer.local().execute(pt,spatialReference, radius, null);
		
		Envelope envelope = new Envelope();
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
		return result;
	}
	
	
	/**
	 * Sets up mapper with filter geometry provided as argument[0] to the jar
	 */
	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();
		
		spatialReference = SpatialReference.create(4326);

		// first pull values from the configuration		
		String featuresPath = config.get("sample.features.input");
		labelAttribute = config.get("sample.features.keyattribute", "GridID");
		latitudeIndex = config.getInt("samples.csvdata.columns.lat", 0);
		longitudeIndex = config.getInt("samples.csvdata.columns.long", 1);
		distance = config.getFloat("radius",20);
		
		FSDataInputStream iStream = null;
		
		try {
			// load the JSON file provided as argument 0
			FileSystem hdfs = FileSystem.get(config);
			iStream = hdfs.open(new Path(featuresPath));
			featureClass = EsriFeatureClass.fromJson(iStream);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		} 
		finally
		{
			if (iStream != null)
			{
				try {
					iStream.close();
				} catch (IOException e) { }
			}
		}
		
		// build a quadtree of our features for fast queries
		if (featureClass != null){
			buildQuadTree();
		}
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		
		/* 
		 * The TextInputFormat we set in the configuration, by default, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// We know that the first line of the CSV is just headers, so at byte offset 0 we can just return
		if (key.get() == 0) return;
		
		
		String line = val.toString();
		String [] values = line.split(",");
		
		// Note: We know the data coming in is clean, but in practice it's best not to
		//       assume clean data.  This is especially true with big data processing
		float latitude = Float.parseFloat(values[latitudeIndex]);
		float longitude = Float.parseFloat(values[longitudeIndex]);
		
		// Create our Point directly from longitude and latitude
		Point point = new Point(longitude, latitude);
		
		// Each map only processes one earthquake record at a time, so we start out with our count 
		// as 1.  Aggregation will occur in the combine/reduce stages
		IntWritable one = new IntWritable(1);
		
		ArrayList<Integer> featureIndex_arr = queryQuadTree(point,distance);
		
		if (featureIndex_arr.size() >= 0){
			for(Integer index:featureIndex_arr){
				String name = featureClass.features[index].attributes.get(labelAttribute).toString();
				
				if (name == null) 
					name = "???";
				
				context.write(new Text(name), one);
			}
			
		} else {
			context.write(new Text("*Outside Feature Set"), one);
		}
	}
}
