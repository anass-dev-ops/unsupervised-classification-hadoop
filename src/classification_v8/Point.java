package classification_v8;

import java.awt.BorderLayout;
import java.awt.Color;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import javax.swing.JFrame;

import org.apache.hadoop.io.Writable;

public class Point implements Writable
{
	public static final int DIMENTION=34354;
	public double arr[] = new double[DIMENTION];
	
	 
	public Point()
	{
		
		for(int i=0;i<DIMENTION;++i)
		{
			arr[i]=0;
		}
	}
	
	public Point(double[] arr) {
		this.arr=arr;
	}
	
	
	public double getx() 
	{
		return arr[0];
	}
		//=========================Distances===============================================
	public static double getEulerDist(Point vec1,Point vec2)
	{
		if(!(vec1.arr.length==DIMENTION && vec2.arr.length==DIMENTION))
		{
			System.exit(1);
		}
		double dist=0.0;
		for(int i=0;i<DIMENTION;++i)
		{
			dist+=(vec1.arr[i]-vec2.arr[i])*(vec1.arr[i]-vec2.arr[i]);
		}
		return Math.sqrt(dist);
	}
	
	public static double getsim(Point vec1,Point vec2)
	{
		if(!(vec1.arr.length==DIMENTION && vec2.arr.length==DIMENTION))
		{
			System.exit(1);
		}
		double val1=0.0,val2=0.0,val3=0.0;
		for(int i=0;i<DIMENTION;++i)
		{
			val1+=(vec1.arr[i]*vec2.arr[i]);
			val2+=vec1.arr[i]*vec1.arr[i];
			val3+=vec2.arr[i]*vec2.arr[i];

		}
		return val1/(Math.sqrt(val2)*Math.sqrt(val3));
	}
	
	public String toString()
	{
		//DecimalFormat df=new DecimalFormat("0.0000");
		String rect=String.valueOf(arr[0]);
		for(int i=1;i<DIMENTION;i++)
		{
			rect+=","+String.valueOf(arr[i]);
		}
		return rect;
	}
	
	public String toString2()
	{
		//DecimalFormat df=new DecimalFormat("0.0000");
		String rect=String.valueOf(arr[0]);
		for(int i=1;i<DIMENTION;i++)
		{
			rect+=" "+String.valueOf(arr[i]);
		}
		return rect;
	}
	
	public String toString3()
	{
		//DecimalFormat df=new DecimalFormat("0.0000");
		
		StringBuffer rec = new StringBuffer();
		for(int i=0;i<DIMENTION;i++)
		{
			
			if(i==DIMENTION-1)
				rec.append(arr[i]);
			else
				rec.append(arr[i]+",");
			
		}
		return rec.toString();
	}
 
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		String str[]=in.readUTF().split(",");
		for(int i=0;i<DIMENTION;++i)
		{
			arr[i]=Double.parseDouble(str[i]);
		}
	}
 
	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(this.toString());
	}
	
	
}

