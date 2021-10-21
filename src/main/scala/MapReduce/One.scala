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
// This class performs the map operation, translating raw input into the key-value with key being the time interval and message type and value being 1.
// It will feed the output to the reducer
class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable] {
  val zero = new IntWritable(0)
  val one = new IntWritable(1)
  val word = new Text
  val config: Config = ConfigFactory.load("application.conf").getConfig("TaskGenerator")

  override
  def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    val input = value.toString().split(" ")(0).split('.')(0)
     val t =  value.toString().split(" ")(2)
     val str = value.toString()
    //val pattern = "/^[a-zA-Z0-9!@#$%^&*()_<>;/~:]$/".r

    val pattern = config.getString("regex_pattern").r
/*      str match {
        case pattern => {
          word.set(t)
          context.write(word, one)

      }*/
     /*val res = pattern.findAllIn(str)
     if (!res.hasNext){
        word.set(t)
        context.write(word,one)
     }*/
     //Pattern matching to match the input strings to a particular regex pattern
       val res =  pattern.findFirstMatchIn(str)
       res match {
         case Some(pattern) => {
           t match {
             case "INFO" => {
               word.set(input+" "+"INFO")
               context.write(word, one)
               word.set(input+" "+"DEBUG")
               context.write(word, zero)
               word.set(input+" "+"WARN")
               context.write(word, zero)
               word.set(input+" "+"ERROR")
               context.write(word, zero)
             }
             case "DEBUG" => {
               word.set(input+" "+"INFO")
               context.write(word, zero)
               word.set(input+" "+"DEBUG")
               context.write(word, one)
               word.set(input+" "+"WARN")
               context.write(word, zero)
               word.set(input+" "+"ERROR")
               context.write(word, zero)
             }
             case "WARN" => {
               word.set(input+" "+"INFO")
               context.write(word, zero)
               word.set(input+" "+"DEBUG")
               context.write(word, zero)
               word.set(input+" "+"WARN")
               context.write(word, one)
               word.set(input+" "+"ERROR")
               context.write(word, zero)
             }
             case "ERROR" => {
               word.set(input+" "+"INFO")
               context.write(word, zero)
               word.set(input+" "+"DEBUG")
               context.write(word, zero)
               word.set(input+" "+"WARN")
               context.write(word, zero)
               word.set(input+" "+"ERROR")
               context.write(word, one)
             }
             case _ =>
           }


         }
         case None =>
       }


  }
}

// This class performs the reduce operation, iterating over the key-value pairs to find the sum of Iterable[IntWritable] with key being the time interval - message type and
// value being the number of that message type in that time intervals.
class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  override
  def reduce(key:Text, values:java.lang.Iterable[IntWritable], context:Reducer[Text,IntWritable,Text,IntWritable]#Context) = {
    val sum = values.asScala.foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}


object One {
  def main(args:Array[String]):Int = {
    logger.info(s"value $args");
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("invalid")
      return 2
    }
    val job = new Job(conf, "message distribution in each interval")
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))
    if (job.waitForCompletion(true)) 0 else 1
  }


}