package nl.surfsara.hadoop.seti.mapreduce

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Reducer}

class RedoParquet {
  def main(args: Array[String]) {
    val conf: Configuration = new Configuration
    conf.set("mapred.task.timeout", "7200000")
    val job: Job = Job.getInstance(conf, "Regenerate json from SETI data archives ...")
    job.setJarByClass(classOf[VerifyDataArchives])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.setMapperClass(classOf[RedoParquetMapper])
    job.setReducerClass(classOf[Reducer[_, _, _, _]])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    job.waitForCompletion(true)
  }
}
