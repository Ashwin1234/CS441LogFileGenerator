package MapReduce
import Generation.LogMsgSimulator.logger
import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.util.matching.Regex

import scala.collection.JavaConverters.*



// This class performs the map operation, translating raw input into the key-value of keys being the type of message and value being 1 
// It will feed the output to the reducer

class MessageTypeMap extends Mapper[Object,Text,Text,IntWritable] {
  val one = new IntWritable(1)
  val word = new Text

  override
  def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    val t =  value.toString().split(" ")(2)
    val str = value.toString()
      word.set(t)
      context.write(word,one)
  }
}

// This class performs the reduce operation, iterating over the key-value pairs to find the sum of Iterable[IntWritable] which is the number of messages of each type 
// in the log files.
class MessageTypeReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  override
  def reduce(key:Text, values:java.lang.Iterable[IntWritable], context:Reducer[Text,IntWritable,Text,IntWritable]#Context) = {
    val sum = values.asScala.foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}


object Three {
  def main(args:Array[String]):Int = {
    logger.info(s"value $args");
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("invalid")
      return 2
    }
    val job = new Job(conf, "message type distribution")
    job.setJarByClass(classOf[MessageTypeMap])
    job.setMapperClass(classOf[MessageTypeMap])
    job.setCombinerClass(classOf[MessageTypeReducer])
    job.setReducerClass(classOf[MessageTypeReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))
    if (job.waitForCompletion(true)) 0 else 1
  }


}