package MapReduce
import Generation.LogMsgSimulator.logger
import com.typesafe.config.{Config, ConfigFactory}

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



// This class performs the map operation, translating raw input into the key-value of keys being the type of message and value being the length of the message
// It will feed the output to the reducer
class LongestMessageMap extends Mapper[Object,Text,Text,IntWritable] {

  val word = new Text
  val config: Config = ConfigFactory.load("application.conf").getConfig("TaskGenerator")


  override
  def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    val t =  value.toString().split(" ")(2)
    var str = value.toString()

    //pattern matching to match the string to the regex pattern
    val pattern = config.getString("regex_pattern").r
    pattern.findFirstMatchIn(str) match {
      case Some(pat) => {
        word.set(t)
        t match {
          case "INFO" => {
            var str = value.toString().split(" ")(6)
            context.write(word,new IntWritable(str.length()))

          }
          case "WARN" => {
            var str = value.toString().split(" ")(6)
            context.write(word,new IntWritable(str.length()))

          }
          case "ERROR" => {
            var str = value.toString().split(" ")(5)
            context.write(word,new IntWritable(str.length()))
          }
          case "DEBUG" => {
            var str = value.toString().split(" ")(5)
            context.write(word,new IntWritable(str.length()))
          }
          case _ =>
        }

      }
      case None =>
    }

  }
}

// This class performs the reduce operation, iterating over the key-value pairs to find the max of Iterable[IntWritable] which is the maximum length of message of each type in
// the log files.
class LongestMessageReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  /*def findMax(x: Int, y: Int): Int = {
    val winner = x max y
    winner
  }*/
  override
  def reduce(key:Text, values:java.lang.Iterable[IntWritable], context:Reducer[Text,IntWritable,Text,IntWritable]#Context) = {
    val max = values.asScala.foldLeft(0){(t,i) => t max i.get }
    context.write(key, new IntWritable(max))

  }
}


object Four {
  def main(args:Array[String]):Int = {
    logger.info(s"value $args");
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("invalid")
      return 2
    }
    val job = new Job(conf, "message type distribution for max length")
    conf.set("mapred.textoutputformat.separatorText", ",")
    job.setJarByClass(classOf[LongestMessageMap])
    job.setMapperClass(classOf[LongestMessageMap])
    job.setCombinerClass(classOf[LongestMessageReducer])
    job.setReducerClass(classOf[LongestMessageReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))

    if (job.waitForCompletion(true)) 0 else 1
  }


}
