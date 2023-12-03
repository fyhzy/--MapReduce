## MapReduce实例

#### (1) 计算成绩最高分和平均分

**Mapper**

public class AverageScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

  @Override

 protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

​    //拿到一行文本内容，转换成String 类型

​    String line = value.toString();

​    //将这行文本切分成单词

​    String[] words = line.split(" ");

​    int score = Integer.parseInt(words[1]);

​    context.write(new Text(words[0]), new IntWritable(score));

​    // shuffle can group them 

  }

}

**Reducer**

public class AverageScoreReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{

  @Override

  protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

​    //定义一个计数器

​    int count = 0;

​    int sum=0;

​    double max = 0;

​    //通过value这个迭代器，遍历这一组kv中所有的value，进行累加

​    for(IntWritable value: values) {

​     sum+=value.get();

​     count++;

​     if(value.get()>max) {

​       max = value.get();

​     }

​    }

​    Double avg = sum*1.0/count;

​    //输出这个单词的统计结果

​    context.write(key, new DoubleWritable(avg));

​    context.write(key, new DoubleWritable(max));

​    

  }

}

#### (2) 按照平均分由低到高进行排序

**Mapper**

public class userloginMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{

  @Override

 protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

​    //拿到一行文本内容，转换成String 类型

​    String line = value.toString();

​    //将这行文本切分成单词

​    String[] words=line.split("\t");

​    double i = Double.parseDouble(words[1]);

​    context.write(new DoubleWritable(i),new Text(words[0]));

  }

}

**Reducer**

public class userloginReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{

  protected void reduce(DoubleWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {

​    for(Text value: values) {

​     context.write(value, key);

​    }

  }

}

