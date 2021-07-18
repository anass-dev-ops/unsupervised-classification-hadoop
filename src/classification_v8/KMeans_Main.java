package classification_v8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class KMeans_Main extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
	      
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		
		int iteration = 0,res;
		do
		{
			System.out.println("=====================ITERATION  : "+(iteration+1)+"============================");
			res = ToolRunner.run(conf, new KMeans_Main(), args);
			iteration ++;
			
		}while(iteration<1);
			
		
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    System.out.println("============Temps d'excution==============\n\n\n\n\n\t\t"+elapsedTime+"ms\n\n\n\n\n=======================");
        System.exit(res);
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// Configuration processed by ToolRunner
        Configuration conf = getConf();
 
        Job job = Job.getInstance(conf, "kmeansTest");
        job.setJarByClass(KMeans_Main.class);
        
        FileSystem fs=FileSystem.get(conf);

        
        // Process custom command-line options
        Path inDir = new Path("/output_1/RES_PREP_2/part-r-00000");
        Path outDir= new Path("/Output_kmeans/res_000");
        fs.delete(outDir,true);
        
        FileInputFormat.addInputPath(job,inDir);    // new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outDir);//new Path(args[2]));
        
        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(Combiner.class);
        //job.setCombinerClass(Combiner.class);
        //job.setReducerClass(ClusterReducer.class);


        //job.setReducerClass(UpdateCenterReducer.class);
        
        //job.setNumReduceTasks(2);//

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
