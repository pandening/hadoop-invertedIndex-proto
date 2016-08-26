package net.hujian.hadoop.protos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Created by hujian on 16-8-26.
 * ok,this is the second map/reduce process for the invertedIndex
 * you should first get the first map/reduce result,then run this
 * class,and you should change the main class when output the jar
 */
public class InvertedIndexResultGenerator {
    public static class ContentMapper extends Mapper<Object,Text,Text,Text>{
        //get the word and filename
        private Text keyWord=new Text();
        //the counter info of this key.
        private Text valueCounter=new Text();

        public void map(Object keyIn,Text valueIn,Context context)
                throws IOException,InterruptedException{
            //get the tokenizer object of the input text
            StringTokenizer itr=new StringTokenizer(valueIn.toString());
            while(itr.hasMoreTokens()){
                keyWord.set(itr.nextToken());
                valueCounter.set(itr.nextToken());
                context.write(keyWord, valueCounter);
            }
        }
    }
    //the mapper,just create the result from combine
    public static class InvertedReducer extends Reducer<Text,Text,Text,Text>{
        //generator->get the result value.
        //the string type of result.
        private String ResultVO_Str;

        public void reduce(Text keyIn,Iterable<Text>valuesIn,Context context)
                throws  IOException,InterruptedException{
            ResultVO_Str=new String();
            for(Text value:valuesIn){
                ResultVO_Str+=value.toString()+";";
            }
            ResultVO_Str=ResultVO_Str.substring(0,ResultVO_Str.length()-1);
            //word filename:counter,filename:counter...
            context.write(keyIn,new Text(ResultVO_Str));
        }
    }

    //setup the job
    public static void main(String[] args)
            throws IOException,InterruptedException,ClassNotFoundException{
        //get a config object
        Configuration config=new Configuration();
        //get the job
        Job job=Job.getInstance(config,"InvertedIndex_2");

        //set the map/combine/reduce
        job.setMapperClass(ContentMapper.class);
        job.setReducerClass(InvertedReducer.class);

        //set the output type of mapper/reducer
        //default->map/reduce output type is same.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the input/output file dir
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //run this job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
