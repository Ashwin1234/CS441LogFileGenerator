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

import scala.collection.mutable.Map
import java.util
import scala.util.matching.Regex
import scala.collection.JavaConverters.*
import scala.collection.immutable.{ListMap, TreeMap}
import math.Ordering.Implicits.infixOrderingOps
import math.Ordered.orderingToOrdered


// This class performs the map operation, translating raw input into the key-value of keys being timestamps and value being 1 if it is an error message
// It will feed the output to the reducer

class IntervalMap extends Mapper[Object,Text,Text,IntWritable]{

  val one = new IntWritable(1)
  val zero = new IntWritable(0)
  val word = new Text
  val config: Config = ConfigFactory.load("application.conf").getConfig("TaskGenerator")
  override
   def map (key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit ={
    val input = value.toString().split(" ")(0).split('.')(0)
    val error = value.toString().split(" ")(2)
    val str = value.toString()
    //val pattern = "/^[a-zA-Z0-9!@#$%^&*()_]$/".r

    //pattern matching to match the string to the regex pattern 
    val pattern = config.getString("regex_pattern").r
    pattern.findFirstMatchIn(str) match {
      case Some(pattern) => {
        word.set(input)
        if(error == "ERROR"){

          context.write(word,one)
        }
        else{
          context.write(word,zero)
        }
      }
      case None =>
    }

  }
}

// This class performs the reduce operation, iterating over the key-value pairs to find the sum of Iterable[IntWritable] which is the number of ERROR messages in the 
// time interval

class ReducerMap extends Reducer[Text,IntWritable,Text,IntWritable] {
  val result_map = Map[String,Int]()
  override
  def reduce(key:Text, values:java.lang.Iterable[IntWritable], context:Reducer[Text,IntWritable,Text,IntWritable]#Context) = {
    val sum = values.asScala.foldLeft(0) { (t,i) => t + i.get }
    result_map+=(key.toString() -> sum.toInt)

  }
// cleanup function to sort the output of reducers by value to find the time interval with highest number of ERROR messages
  override
  def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val list = result_map.toList.sortWith((x,y) => y._2<x._2)
    list.foreach(i =>
      context.write(new Text(i(0)), new IntWritable(i(1)))
    )
  }
}

object Two {
  def main(args:Array[String]):Int = {
    logger.info(s"value $args");
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("invalid")
      return 2
    }
    val job = new Job(conf, "Error message in each interval")
    job.setJarByClass(classOf[IntervalMap])
    job.setMapperClass(classOf[IntervalMap])
    job.setCombinerClass(classOf[ReducerMap])
    job.setReducerClass(classOf[ReducerMap])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))
    if (job.waitForCompletion(true)) 0 else 1
  }
}
