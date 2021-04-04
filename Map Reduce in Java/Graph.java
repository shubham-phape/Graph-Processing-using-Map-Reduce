import java.io.*;
import java.time.temporal.ValueRange;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.Vector;

import org.apache.commons.math3.analysis.function.Min;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex extends IntWritable implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent= new Vector<>();     // the vertex neighbors

    public Vertex() {
    }

    public Vertex(short tag, long group, long VID, Vector<Long> adjacent) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    public Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
    }
//getter and setter methods
    public long getVID() {
        return VID;
    }

    public long getGroup() {
        return group;
    }

    public short getTag() {
        return tag;
    }

    public Vector<Long> getAdjacent() {
        return adjacent;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeShort(this.tag);
        dataOutput.writeLong(this.group);
        dataOutput.writeLong(this.VID);
        //looping to write each element of Vector array
        for (Long aLong : this.adjacent) {
            dataOutput.writeLong(aLong);
        }

    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.tag= dataInput.readShort();
         this.group=dataInput.readLong();
         this.VID= dataInput.readLong();
         this.adjacent= read_theVector(dataInput);
    }
//function to read each adjacent element entry in the array
    public static Vector<Long> read_theVector(DataInput datainput) throws IOException {
        Vector<Long> return_vector = new Vector<>();
        long var;
        int i=1;
        while (i > 0 ){
            try {
                if ((var= datainput.readLong())!= -1){
                    return_vector.add(var);
                }
                else {
                    i=0;
                }
            }
            catch (EOFException eof){
                i=0;
            }
        }
        return return_vector;
    }

}

public class Graph {

    /* ..First Mapper class. */
    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Vertex> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String input_line= value.toString();
            String[] each_node= input_line.split(",");
            long VID= Long.parseLong(each_node[0]);
            Vector<Long> adj_nodes= new Vector<>();
            for (int i=1; i< each_node.length; i++){
                adj_nodes.add(Long.parseLong(each_node[i]));
            }



            context.write( new LongWritable(VID), new Vertex((short) 0, VID, VID, adj_nodes));
        }
    }
    /* ..Second Mapper class. */
    public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

        public void map(LongWritable key, Vertex value, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(value.getVID()), value);

            for (Long n: value.getAdjacent())
            {
                context.write(new LongWritable(n),new Vertex((short) 1, value.getGroup()));
            }
        }
    }
    /* ...Second Reducer class.....*/
    public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

        public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException {
            Vector<Long> adj= new Vector<>();
            long m= Long.MAX_VALUE;
            for (Vertex v : values){
                if (v.getTag()==0)
                    adj= v.adjacent;
                m= Math.min(m, v.getGroup());
            }
        context.write(new LongWritable(m),new Vertex((short) 0,m, key.get(),adj));

        }
    }
    /*  .... Last mapper class...    */
    public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {

        public void map(LongWritable key, Vertex value, Context context)
                throws IOException, InterruptedException {

            context.write(key, new LongWritable(1));
        }
    }
    /*      ....Last Reducer class..  */
    public static class FinalReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long m=0;
            for (LongWritable v: values) {
                m = m + v.get();
            }
            context.write(key,new LongWritable(m));
        }
    }



    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("First Job");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(FirstMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/f0"));
        job.waitForCompletion(true);

        for ( short i = 0; i < 5; i++ ) {
            Job job1 = Job.getInstance();
            // ... Second Map-Reduce job to propagate the group number
            job1.setJobName("Second Job");
            job1.setJarByClass(Graph.class);

            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Vertex.class);
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Vertex.class);
            job1.setMapperClass(SecondMapper.class);
            job1.setReducerClass(SecondReducer.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job1, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f" + (i + 1)));

            job1.waitForCompletion(true);
        }
         Job job2 = Job.getInstance();
        job2.setJobName("Final Job");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
    }
}
