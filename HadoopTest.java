/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadooptest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 *
 * @author David
 */

public class HadoopTest extends Configured implements Tool{

     static class MapFilter extends Mapper<LongWritable, Text, CompositeKey, DoubleWritable> {
      
        private String result_filter = "solved";
       
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Splittiong the output
            String data = value.toString();
            String[] fields = data.split("\t",-1);
            //filtering by solved
            String result = fields[14];       
            if(result.matches(result_filter) && !fields[0].matches("Solver")){
                
                CompositeKey keyvalue = new CompositeKey(new Text(fields[0]), new DoubleWritable(Double.valueOf(fields[11])));              
                context.write(keyvalue, new DoubleWritable(Double.valueOf(fields[11])));
            }
        }
    }
     
    ////////Write the pair values Solver - Time sorted ascendand
    static class ReducerSorter extends Reducer<CompositeKey, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(CompositeKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {          
            
            for (DoubleWritable value : values) {
                context.write(key.key, value);
            }          
        }     
    }
    
    
    /////////Convert the Solver - Times in  single row

    public static class SingleRowMapper extends Mapper <LongWritable, Text, LongWritable, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            long column =0;
            long row = key.get();
            String[] Values = value.toString().split(" ");
            for(String SValue : Values){
                context.write(new LongWritable(column), new Text(row + "\t" + SValue));
                ++column;
            }
        }
    }
    
    ////////Write the Singles Rows
    public static class SingleRowReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {          
            TreeMap<Long, String> SingleRow = new TreeMap<Long, String>();
             

            while (values.iterator().hasNext()) {
                String rowValue = values.iterator().next().toString();
                String[] rowSplits = rowValue.split("\t");

                SingleRow.put(Long.valueOf(rowSplits[0]), rowSplits[1]);
            }

            String rowString = StringUtils.join("\t", SingleRow.values());
            context.write(new Text(rowString), NullWritable.get());
            
            /*for (Text value : values) {
                context.write(value, NullWritable.get());
            } */ 
        }
    }
    
    
    
    public int run(String[] args) throws Exception {
        JobControl jobControl  = new JobControl("jobChain");
        Configuration conf1 = getConf();
        
        
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(HadoopTest.class);
        job1.setJobName("Filter and Sort");
        
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
        
        job1.setMapperClass(MapFilter.class);
        job1.setReducerClass(ReducerSorter.class);               
        job1.setGroupingComparatorClass(KeyComparator.class);
        
        job1.setOutputKeyClass(CompositeKey.class);
        job1.setOutputValueClass(DoubleWritable.class); 
        
        ControlledJob controlledJob1  = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        
        jobControl.addJob(controlledJob1);
        Configuration conf2 = getConf();
        
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(HadoopTest.class);
        job2.setJobName("Create Single Row");
        
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
        
        job2.setMapperClass(SingleRowMapper.class);
        job2.setReducerClass(SingleRowReducer.class);
        
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
        
        controlledJob2.addDependingJob(controlledJob1); 
        jobControl.addJob(controlledJob2);
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        
        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
            }
        } 
        System.exit(0);  
        return (job1.waitForCompletion(true) ? 0 : 1);   
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int exitCode = ToolRunner.run(new HadoopTest(), args);  
         System.exit(exitCode);

    }
    
    ///////// Secondary Sort Classes
    public static class CompositeKey implements Writable, WritableComparable<CompositeKey> {

        private Text key;
        private DoubleWritable value;

        public CompositeKey() {
            this.key = new Text();
            this.value = new DoubleWritable();
        }

        public CompositeKey(Text key, DoubleWritable value) {
            this.key = key;
            this.value = value;
        }

        public void set(Text key, DoubleWritable value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            key.readFields(in);
            value.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            key.write(out);
            value.write(out);
        }

        @Override
        public int compareTo(CompositeKey o) {
            int compareValue = this.key.compareTo(o.key);
            if (compareValue == 0) {
                compareValue = value.compareTo(o.value);
            }
            return compareValue;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CompositeKey) {
                CompositeKey aux = (CompositeKey) o;
                return key.equals(aux.key) && value.equals(aux.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }    
    static class KeyComparator extends WritableComparator {
 
      public KeyComparator() {
          super(CompositeKey.class, true);
      }

     @Override
    
     public int compare(WritableComparable wc1, WritableComparable wc2) {
         CompositeKey pair = (CompositeKey) wc1;
         CompositeKey pair2 = (CompositeKey) wc2;
         return pair.key.compareTo(pair2.key);
     }
   }  
    
    
    ////hadoop jar D:\Estudio\Big_Data\Codigo\HadoopTest\dist\HadoopTest.jar D:\Estudio\Big_Data\input D:\Estudio\Big_Data\output
 }
    

