/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadooptest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/**
 *
 * @author David
 */

public class HadoopTest {

     static class MapFilter extends Mapper<LongWritable, Text, CompositeKey, DoubleWritable> {
      
        //private Text word = new Text();
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

    static class ReducerSorter extends Reducer<CompositeKey, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(CompositeKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            
            
            for (DoubleWritable value : values) {
                context.write(key.key, value);
            }          
        }     
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        job.setMapperClass(MapFilter.class);       
        job.setReducerClass(ReducerSorter.class);
        
        
        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        job.setGroupingComparatorClass(KeyComparator.class);
        
        FileInputFormat.setInputPaths(job, new Path(myArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(myArgs[1]));

        job.waitForCompletion(true);
        System.exit(0);

    }
    
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
 }
    

