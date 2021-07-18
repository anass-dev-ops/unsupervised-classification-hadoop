package classification_v8;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Progressable;



public class Combiner extends Reducer<Text, Text, Text, Text> {
	
	
	
	int k;
	Vector<Point> vecteures;

	Vector<Point> newCentres = new Vector<Point>(); 
	
	//VALIDATION
	double TCR,jaccard,jaccardM=0;int min,max;
	
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
    {
		
		System.out.println("\n\nCluster "+key.toString()+" : \n");
		
		//VALIDATION
		if(Integer.parseInt(key.toString())==0) {min=1;max=1000;}
		if(Integer.parseInt(key.toString())==1) {min=1001;max=2000;}
		if(Integer.parseInt(key.toString())==2) {min=2001;max=3000;}
		
		TCR = max-min+1;
		
		Point sumPoint=new Point();
		Point newCenterPoint=new Point();

		int count=0;
		//VALIDATION
		double VP=0.0,FP=0.0,FN=0.0;
		String LineDoc;
		double[] Vect = new double[Point.DIMENTION];
        while(values.iterator().hasNext())
        {
        	String[] VectDoc_index = values.iterator().next().toString().split("&"); //nom_fichier = id_fichier
        	System.out.print(VectDoc_index[1]+" ; ");
        	
        	LineDoc = VectDoc_index[0];			
        	Vect = LineToVect(LineDoc);
			//VALIDATION
			
			if(Integer.parseInt(VectDoc_index[1]) >= min && Integer.parseInt(VectDoc_index[1]) <= max) {
				VP++;
			}
			else  {
				FP++;
			}
		     for(int i=0;i<Point.DIMENTION;i++)
	             {
	                 sumPoint.arr[i]+=Vect[i];// sum les Xs , sum les Ys
	                 
	 			}      
            count++;
        }
        
        for(int i=0;i<Point.DIMENTION;i++)
        {
            newCenterPoint.arr[i] = sumPoint.arr[i]/count;
		}
        newCentres.add(newCenterPoint);
        
      //VALIDATION
        FN=TCR-VP;
       jaccard = VP/(VP+FP+FN); 		jaccardM+=jaccard;

       System.out.println("\n coefficient de Jaccard " + jaccard);
        
        context.write(key, new Text(newCenterPoint.toString3()));
		

		
    }
	
	
	@Override
    public void cleanup(Context context) throws IOException,InterruptedException 
    {
		Path pt = new Path("hdfs:/input_kmeans_2/Centres_input.txt");
		FileSystem fs = FileSystem.get(new Configuration());
		if ( fs.exists( pt )) { fs.delete( pt, true ); } 

		//Init output stream
		FSDataOutputStream outputStream=fs.create(pt);
		//Cassical output stream usage
		
		k=newCentres.size();
		//System.out.println("\n\n\n\n\n\\t\t\t newCentres.size()"+newCentres.size());
		for(int i=0;i<3;i++) {
			String newCentersS = newCentres.get(i).toString3();
			String[] str = newCentersS.split(",");
			//String line="99\t";
			StringBuffer line = new StringBuffer("99\t");
			for(int j=0;j<str.length;j++) {
				
				if(Double.parseDouble(str[j])!=0.0) {
					//line+=((j+1)+":"+str[j]+",");
					line.append((j+1)+":"+str[j]+",");
				}
			}
			if(i==2)
				outputStream.writeBytes(line.substring(0, line.length()-1));
				//br.write(line.substring(0, line.length()-1));
			else
				outputStream.writeBytes(line.substring(0, line.length()-1)+"\n");
				//br.write(line.substring(0, line.length()-1)+"\n");
		
		}
		outputStream.close();
		
		
		
		//VALIDATION
		jaccardM =jaccardM/3;
		System.out.println("jaccard total = "+ jaccardM);
		

    }
	
	
	
	
	public double[] LineToVect(String line) {
		   
		   //Point point = new Point();
		   double[] VectDoc = new double[Point.DIMENTION];
		   for(int i=0;i<VectDoc.length;i++) {
			   VectDoc[i]=0.0;
		   }
		   
		   String[] item_IdM_W = line.split(",");
		   
		   
		   for(int i=0;i<item_IdM_W.length;i++) {
			   String[] IdM_W = item_IdM_W[i].split(":");
			   VectDoc[Integer.parseInt(IdM_W[0])-1]=Double.parseDouble(IdM_W[1]);

		   }
		   
		  
		   
		   return VectDoc;
		   
	   }
   
		
}
