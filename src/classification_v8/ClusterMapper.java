package classification_v8;



import org.apache.hadoop.conf.Configuration;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ClusterMapper extends Mapper<LongWritable, Text, Text, Text>{

	int k=0;
	
	Vector<Point> centers;
	Vector<String> centresS;
	
	
	@Override
    public void setup(Context context) throws IOException{
		centers = new Vector<Point>();
		centresS = new Vector<String>();
		try {
			Path pt=new Path("hdfs:/input_kmeans_2/Centres_input.txt");//Location of file in HDFS
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			
			//System.out.println("Les Centres : ");
			String line;
			while ((line=br.readLine())!=null)
	    	{
				Point point = new Point();
	        	String str[] =line.split("\t");
	            //System.out.print("\n\n\n\n\n\t\t\t"+str.length+"\n\n\n\n\n");
				point = LineToVect(str[1]);
	        	
				centers.add(point);
				centresS.add(str[1]);
				k++; 				
	    	}
			
		}catch(Exception e) {}	

    }
   	
	
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	   
		   
			
		   
		   if(value.toString().isEmpty()) {
			   return;
		   }
		   
		   Point point = new Point();	
		   double maxDist=0.0;//Double.MAX_VALUE;
		   int index = -1;
		   
		   double[] VectDoc = new double[Point.DIMENTION];
		   for(int i=0;i<VectDoc.length;i++) {
			   VectDoc[i]=0.0;
		   }
		   
		   String[] keyval = value.toString().split("\t");
		   
		   String key1 = keyval[0];
		   //String[] item_IdM_W = keyval[1].split(",");
		   
		   point = LineToVect(keyval[1]);

           for(int i=0;i<k;i++)
           {
               double dist=Point.getsim(point, centers.get(i));
               if(dist>maxDist)
               {
                   maxDist=dist;
                   index=i;
               }
           }
           
           
           //context.write(new Text(index+""), new Text("Doc : "+key1));
           context.write(new Text(index+""), new Text(keyval[1]+"&"+key1));
    	
	   }
	   

	   public Point LineToVect(String line) {
		   
		   Point point = new Point();
		   double[] VectDoc = new double[Point.DIMENTION];
		   for(int i=0;i<VectDoc.length;i++) {
			   VectDoc[i]=0.0;
		   }
		   
		   String[] item_IdM_W = line.split(",");
		   
		   
		   for(int i=0;i<item_IdM_W.length;i++) {
			   String[] IdM_W = item_IdM_W[i].split(":");
			   VectDoc[Integer.parseInt(IdM_W[0])-1]=Double.parseDouble(IdM_W[1]);

		   }
		   
		   for(int i=0;i<Point.DIMENTION;i++)
           {
               point.arr[i]=VectDoc[i];        
           }
		   
		   return point;
		   
	   }
	   
}
