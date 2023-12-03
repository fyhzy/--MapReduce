## MapReduce实例(自定义类型实现)

### **按月份分区统计2016年每个月每个用户的登录次数**

**People.java**

```java
public class people implements WritableComparable<people> {
	private String name;
	private String logTime;
    public people() {}
    public people(String name,String logTime) {
        setName(name); 
        setLogTime(logTime);
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getLogTime() {
        return logTime.substring(0, 7);
    }
    public void setLogTime(String logTime) {
        this.logTime = logTime.substring(0, 7);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        logTime = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(logTime);
    }
    @Override
    public String toString() {
        return name + "," + logTime;
    }
    public int compareTo(people p) {
        int nameCompare = this.name.compareTo(p.getName());
        if(nameCompare!=0) {
            return nameCompare;
        }else {
            return this.logTime.compareTo(p.getLogTime());
        }
    }
}
```

**Mapper.java**

```java
public class userloginMapper extends Mapper<LongWritable, Text, people, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value,Context context) throws InterruptedException, IOException {
		//拿到一行文本内容，转换成String 类型
		String line = value.toString();
		//将这行文本切分成单词
		String[] words=line.split(",");
		people p = new people();
		p.setName(words[0]);
		p.setLogTime(words[1]);
		context.write(p, new IntWritable(1));
	}
}
```

**Reducer.java**

```java
public class userloginReducer extends Reducer<people, IntWritable, people, IntWritable>{
	@Override
	protected void reduce(people key, Iterable<IntWritable> values,Context context) throws InterruptedException, IOException {
		int sum=0;
		for(IntWritable value: values) {
			sum+=value.get();
		}
		context.write(key, new IntWritable(sum));	
	}
}
```

**Combiner.java**

```java
public class PeopleLogtimeCombiner extends Reducer<people, IntWritable, people, IntWritable>{
	@Override
	protected void reduce(people key, Iterable<IntWritable> values,Context context) throws InterruptedException, IOException {
		int sum=0;
		for(IntWritable value: values) {
			sum+=value.get();
		}
		context.write(key, new IntWritable(sum));	
	}
}
```

**Partitioner.java**

```java
public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<people,IntWritable> {
public int getPartition(people key, IntWritable value,int numPartitions) {
		String date = key.getLogTime();
		int month = Integer.parseInt(date.substring(5));
		return month%numPartitions;
	}
}
```

**Comparator.java**

```java
public class Comparator extends WritableComparator{
    protected Comparator(){
        super(people.class,true);
    }
    public int compare(WritableComparable w1, WritableComparable w2) {
        people p1 = (people)w1;
        people p2 = (people)w2;
        if(p1.getName().compareTo(p2.getName())!=0) {		
            return -1*p1.getName().compareTo(p2.getName());			
        }else {		
            return -1*p1.getLogTime().compareTo(p2.getLogTime());	
        }
    }
}
```
**Driver.java**

```java
public static void main(String[] args) throws Exception{
	Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job userloginJob = Job.getInstance(conf,"user logTime");
	//重要：指定本job所在的jar包
    userloginJob.setJarByClass(userlogin.class);

    //设置wordCountJob所用的mapper逻辑类为哪个类
    userloginJob.setMapperClass(userloginMapper.class);
    //设置wordCountJob所用的reducer逻辑类为哪个类
    userloginJob.setReducerClass(userloginReducer.class);

    //设置map阶段输出的kv数据类型
    userloginJob.setMapOutputKeyClass(people.class);
    userloginJob.setMapOutputValueClass(IntWritable.class);

    userloginJob.setCombinerClass(PeopleLogtimeCombiner.class);

    userloginJob.setPartitionerClass(Partitioner.class);
    userloginJob.setNumReduceTasks(12);

    userloginJob.setSortComparatorClass(Comparator.class);
    //设置最终输出的kv数据类型
    userloginJob.setOutputKeyClass(people.class);
    userloginJob.setOutputValueClass(IntWritable.class);
    //设置要处理的文本数据所存放的路径
	FileInputFormat.setInputPaths(userloginJob, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(userloginJob, new Path(otherArgs[1]));
    //提交job给hadoop集群
    userloginJob.waitForCompletion(true);
}
```
1. 