package de.l3s.souza.indexSolrHadoop;
//de.l3s.souza.indexSolrHadoop.XmlSlorReader
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.elasticsearch.hadoop.mr.EsOutputFormat;


public final class XmlSolrReader extends Configured implements Tool {
	
	private static Configuration conf;

	/*
	 public static class RecordSerializer implements JsonSerializer<Record> {
	        public JsonElement serialize(final Record record, final Type type, final JsonSerializationContext context) {
	            JsonObject result = new JsonObject();
	            result.add("annotations", new JsonPrimitive(record.getAnnotations()));
	            result.add("article", new JsonPrimitive(record.getArticle()));
	           
	            return result;
	        }

			@Override
			public JsonElement serialize(Record src,
					java.lang.reflect.Type typeOfSrc,
					JsonSerializationContext context) {
				// TODO Auto-generated method stub
				return null;
			}
	    }
	 */

public static class SampleMapper extends Mapper<Object, Text, Text, MapWritable > { 
	
	
    private final NullWritable outKey = NullWritable.get();
  
    private static String article;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException 
    {
    	
    }
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		boolean firstTimeContentVisited = true; 
		String id = "";
		String contentTag = "";
		String str = value.toString();
		str = str + "\n</doc></add>";
		StringTokenizer token = new StringTokenizer (str,"\n"); 
	
		/*
		 * 	<field name="url">http://www.bbc.co.uk/news/uk-england-south-yorkshire-15692495</field>
	<field name="title">Sheffield City Council proposes 690 job cuts</field>
	<field name="sourcerss">http://feeds.bbci.co.uk/news/england/rss.xml</field>
	<field name="source-encoding">UTF-8</field>
	<field name="id">lk-20111112040101_3322</field>
	<field name="host">www.bbc.co.uk</field>
	<field name="date">2011-11-11T00:00:00Z</field>
	<field name="content">
<subtopic id=\"(.+?)\""
		 * */
		MapWritable doc = new MapWritable();
		
		while (token.hasMoreTokens())
		{
			String line = token.nextToken();
			if (line.contains("<field name=\"id\">"))
			{
				final Pattern pattern = Pattern.compile("<field name=\"id\">(.+?)</field>");
				final Matcher m = pattern.matcher(line);
				m.find();
				id = m.group(1);
				doc.put(new Text("id"), new Text(id));
				
			}
			
			if (line.contains("<field name=\"url\">"))
			{
				final Pattern pattern = Pattern.compile("<field name=\"url\">(.+?)</field>");
				final Matcher m = pattern.matcher(line);
				m.find();
				String url = m.group(1);
				doc.put(new Text("ourl"), new Text(url));

				
			}
			if (line.contains("<field name=\"title\">"))
			{
				final Pattern pattern = Pattern.compile("<field name=\"title\">(.+?)</field>");
				final Matcher m = pattern.matcher(line);
				m.find();
				String title = m.group(1);
				doc.put(new Text("title"), new Text(title));
				
			}
			if (line.contains("<field name=\"sourcerss\">"))
			{
				final Pattern pattern = Pattern.compile("<field name=\"sourcerss\">(.+?)</field>");
				final Matcher m = pattern.matcher(line);
				m.find();
				String sourcerss = m.group(1);
				doc.put(new Text("sourcess"), new Text(sourcerss));
				
			}
			if (line.contains("<field name=\"host\">"))
			{
				final Pattern pattern = Pattern.compile("<field name=\"host\">(.+?)</field>");
				final Matcher m = pattern.matcher(line);
				m.find();
				String host = m.group(1);
				doc.put(new Text("host"), new Text(host));
				
			}
			if (line.contains("<field name=\"date\">"))
			{
				final Pattern pattern = Pattern.compile("<field name=\"date\">(.+?)</field>");
				final Matcher m = pattern.matcher(line);
				m.find();
				String date = m.group(1);
				doc.put(new Text("ts"), new Text(date));
				
			}
			//<field name="content">
			
			if (line.contains("<field name=\"content\">") && firstTimeContentVisited)
			{
				
				while (token.hasMoreTokens())
				{
					contentTag = contentTag + token.nextToken();
				}
				contentTag = line + contentTag;
				firstTimeContentVisited = false;
			}
			
			if (line.contains("<field name=\"content\">") && !(firstTimeContentVisited))
			{
			  try
			  {
				final Pattern pattern = Pattern.compile("<field name=\"content\">(.+?)</field>");
				final Matcher m = pattern.matcher(contentTag);
				m.find();
				String content = m.group(1);
				doc.put(new Text("text"), new Text(content));
			  } catch (Exception e)
			  {
				  doc.put(new Text("text"), new Text(""));
			  }
			}
			
			
		//JsonElement jelement = new JsonParser().parse(record);
		}
		
		context.write(new Text(id), doc);
		}
	
}
public static void main(String[] args) throws Exception {

	 int res = ToolRunner.run(conf,new XmlSolrReader(), args);
    System.exit(res);


}

//@Override
public int run(String[] args) throws Exception {
	
	Path inputPath = new Path(args[0]);
	//Path outputDir = new Path(args[1]);

	System.setProperty("hadoop.home.dir", "/");
	// Create configuration
	
	Job job = Job.getInstance(getConf());
	

	job.setJarByClass(XmlSolrReader.class);
	job.setJobName("elastic-search-reindex");
	conf = job.getConfiguration();
	conf.set("textinputformat.record.delimiter","</doc>\n");
	conf.set("es.nodes","master02.ib");
	conf.setLong(YarnConfiguration.NM_PMEM_MB, 58000);
	conf.setLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 3000);
	conf.setLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 58000);
	conf.setLong(MRJobConfig.MAP_MEMORY_MB, 3000);
	conf.setLong(MRJobConfig.REDUCE_MEMORY_MB, 6000);
	conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx2400m");
	conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx4800m");
	conf.setLong("yarn.app.mapreduce.am.resource.mb", 10000);
	conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx8000m");
	conf.setLong(MRJobConfig.IO_SORT_MB, 1000);
	//conf.setBoolean("mapreduce.map.speculative", true);    
	//conf.setBoolean("mapreduce.reduce.speculative", true);
	conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 5);
	conf.setInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, 10);
	conf.setInt(MRJobConfig.JVM_NUMTASKS_TORUN, 200);
	conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 5);
	// conf.setInt("yarn.app.mapreduce.am.resource.mb", 10_000);
	conf.set("es.resource", "souza_livingknowledge_2/capture");
//	conf.set("es.mapping.id", "id");
	conf.set("es.batch.size.bytes", "200mb");
	//conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	//conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
	//conf.set("es.input.json", "yes");
	//conf.set("shield.user", "souza:pri2006");
	//  conf.set(k	"es.net.http.auth.user", "souza");
	// conf.set("es.net.http.auth.pass", "pri2006");
	conf.set("es.batch.size.entries","100000");
	//conf.set("es.port", "9200");
	conf.set("es.port", "9205");
	conf.setBoolean("mapreduce.job.user.classpath.first", true);
	conf.set("yarn.app.mapreduce.am.log.level", "OFF");
	conf.set("mapreduce.map.log.level", "OFF");
	conf.set("mapreduce.reduce.log.level", "OFF");// Setup MapReduce
	// Setup MapReduce
	job.setMapperClass(SampleMapper.class);
	job.setReducerClass(Reducer.class);
	job.setNumReduceTasks(30);
//de.l3s.souza.indexSolrHadoop.XmlSlorReader
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(MapWritable.class);


	// Input
	FileInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(TextInputFormat.class);

	// Output
	//FileOutputFormat.setOutputPath(job, outputDir);
	job.setOutputFormatClass(EsOutputFormat.class);
	// Delete output if exists
	FileSystem hdfs = FileSystem.get(conf);
/*	if (hdfs.exists(outputDir))
	hdfs.delete(outputDir, true);
*/
	//FileOutputFormat.setCompressOutput(job, true);
	//FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	// Execute job
	return job.waitForCompletion(true) ? 0 : 1;
	
}




}
