package com.hniu.bigdata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
//import java.net.URI;
import java.util.Iterator;
//import java.util.StringTokenizer;
public class NewWordCount {
    public static class TokenizerMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str = line.split(" ");
            for (String e : str) { //new Text(e);
                context.write(new Text(e), new IntWritable(1));}}}
    public static class IntSumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable vr;
            int sum=0;
            for(Iterator a=values.iterator();a.hasNext();sum+=vr.get()){
                vr =(IntWritable) a.next();
                }
            this.result.set(sum);
            context.write(key, this.result); }}
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(NewWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(NewWordCount.IntSumReduce.class);
        job.setCombinerClass(NewWordCount.IntSumReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("D:/wordcount/input/core-site.xml"));
        Path outPath = new Path("D:/wordcount/output");
        if (fs.exists(outPath)) { fs.delete(outPath, true); }
        FileOutputFormat.setOutputPath(job, outPath);
        boolean completion = job.waitForCompletion(true);
        if (completion) { System.out.println("完成"); } }}
































//            String[] str = line.split(" ");
//            for (String e : str) {
//            str.set();
//                context.write(new Text(e), new IntWritable(1));
//            word.set(str[0]);
//            StringTokenizer itr = new StringTokenizer(value.toString());

//            while (itr.hasMoreTokens()) {
//                this.word.set(itr.nextToken().split(",")[1]);
//                context.write(this.word, one);
///**
// * The entry point of application.
// *
// * @param args the input arguments
// * @throws IOException            the io exception
// * @throws ClassNotFoundException the class not found exception
// * @throws InterruptedException   the interrupted exception
// */

//        if (args.length != 2) {
//            System.err.println("Usage:Merge and duplicate removal <in> <out>");
//            System.exit(2);
//        }
//        FileSystem fileSystem = FileSystem.get(URI.create(args[1]), conf);
//        if(fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }


//        FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//            String line = value.toString();
//            String[] str = itr.split("，");
//            for (String e: str) {
////            str.set();
//            context.write(new Text(e), new IntWritable(1));
//                new Text(e);
//                context.write(new Text(str[1]),one);
//            for (Iterator i = values.iterator(); i.hasNext(); sum += val.get()) {
//                val = (IntWritable) i.next();
////            }












