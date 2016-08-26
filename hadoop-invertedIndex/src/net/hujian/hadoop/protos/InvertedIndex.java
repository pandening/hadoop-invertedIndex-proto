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
 * map/reduce proto
 */
public class InvertedIndex {
    //the mapper,this mapper will get the contents from file.then
    //map it to combine,let the combine count the words,then the
    //reducer will get the information that contain->word,counter,file
    //so,the reducer just create the file list(result)
    //you should understand hadoop's  shuffle process.
    //map-shuffle-combine-shuffle-reducer
    public static class ContentMapper extends Mapper<Object,Text,Text,Text>{
        //get the word and filename
        private Text keyWordFilename=new Text();
        //the counter info of this key.
        private Text valueCounter=new Text();
        //we need get the filename by the split object
        private InputSplit split;

        public void map(Object keyIn,Text valueIn,Context context)
                throws IOException,InterruptedException{
            //get the split of this item
            split=context.getInputSplit();
            //get the tokenizer object of the input text
            StringTokenizer itr=new StringTokenizer(valueIn.toString());
            while(itr.hasMoreTokens()){
                //the kayOut contains [word:filename]
                //the valueOut is a number means the counter
                //keyWordFilename.set(itr.nextToken()+":"+split.getPath().toString());
                keyWordFilename.set(itr.nextToken() + ":"+((FileSplit)(split)).getPath().getName().toString());
                valueCounter.set("1");
                context.write(keyWordFilename,valueCounter);
            }
        }
    }

    //the combine,just get the counter information
    //BUT,NOTICE,THE COMBINE WILL BE NOT RUN AT ALL,OR RUN AFTER MAPPER AND
    //REDUCER...SO JUST IGNORE THIS COMBINE,I WILL USE ANOTHER HADOOP JOB
    //TO GET THE RESULT FILE.
    public static class CounterCombine extends Reducer<Text,Text,Text,Text>{
        //this is the statistic information
        private Text valueOutInfo=new Text();

        public void reduce(Text keyIn,Iterable<Text>valueIn,Context context)
            throws IOException,InterruptedException{
            //get the counter information
            int Counter=0;
            for(Text v:valueIn){
                Counter+=Integer.parseInt(v.toString());
            }
            //re-set the valueOut
            valueOutInfo.set(keyIn.toString().substring(keyIn.toString().indexOf(':')+1)+":"+Counter);
            //re-set the key.
            keyIn.set(keyIn.toString().substring(0, keyIn.toString().indexOf(':')));
            context.write(keyIn,valueOutInfo);
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
            //remove the last ','
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
        Job job=Job.getInstance(config,"InvertedIndex_1");

        //set the map/combine/reduce
        job.setMapperClass(ContentMapper.class);
        job.setReducerClass(CounterCombine.class);

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
