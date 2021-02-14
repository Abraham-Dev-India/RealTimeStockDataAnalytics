# RealTimeStockDataAnalytics
A project to do real time stream analytics on Stock Market Data using Spark and Kafka

1) Set a unique name for the appplication and set Master as local using SparkConf
2) Create a JavaStreamingContext with a batch interval of 60 seconds
3) Define a HashMap to hold the kafka parameters
4)  Create direct kafka stream with brokers and topics/LocationStrategy with prefer consistent 
    allows partitions to be distributed consistently to the spark executors.
5) CosumerStrategy allows to subscribe to the kafka topics.
6) Create a JavaInputDStream. It is a continuous input stream associated to the source.
7)  Create a JavaDStream named "lines" and is mapped to the Java Objects that were created from the json Schema using JackSon library.  The Java class holding the json schema is named StockData
8)  Using the created JavaDStream(lines), open a window of 600 seconds and a sliding window of 300 seconds. The analytics is done within this window




*************************************** Analysis 1 - Simple Moving Average *************************************************************
1) Create a tuple from the stream in the format Tuple2<String,Double>,where String -> Symbol and Double -> ClosePrice
2) A new RDD in the format - JavaPairRDD<String, Tuple2<Double, Double>>, where String -> Symbol and Tuple2 -> <ClosePrice,1.00>,
is created usng mapToPair transformation
3) A new RDD in the format - JavaPairRDD<String, Tuple2<Double, Double>>, where String -> Symbol and Tuple2 ->  <Sum of all ClosePrice, No of record for the particular stock>, is created using reduceByKey transformation
4) A new RDD in the format - JavaPairRDD<String, Double>,where String -> Symbol and Double -> Average, is created using mapToPair transforamtion. The average is calculated by the function - getAverageByKey





******************************************** Analysis 2 - Maximum Profit ***********************************************************************
1) The above mentioned steps are done for open price to get the Average Open Price
2) The two RDDs are joined to form a new RDD in the format JavaPairRDD<String, Tuple2<Double, Double>>, where String -> Symbol and Tuple2 -> <closePriceAverage,openPriceAverage>
3) A new RDD in the format - JavaPairRDD<String, Double>, where String -> Symbol and Double -> profit, is created using map transformation by subtracting (ClosePrice - OpenPrice)
4) Finally, a new Tuple is created in the format Tuple2<String, Double>, where String -> Symbol and Double -> maxprofit,  is created using the rdd.max function. The comparator function is held inside the class named TupleComparator
	   
****************************************** Analysis 3 - Maximum Volume **************************************************************************
i) The above mentioned steps are used to find the volume of each stock
ii) A new RDD in the format - JavaPairRDD<String, Double>,where String -> Symbol and Double -> sum of all volumes, is created using reduceByKey transformation
iii) A new Tuple is created in the format Tuple2<String, Double>, where String -> Symbol and Double -> maxvolume, is created using the rdd.max function. The comparator function is held inside the class named TupleComparator	   

COMMAND TO RUN SparkApplication.jar fat jar
java -jar <full path of jar file> 52.55.237.11:9092 123 stockData

STEPS TO RUN FROM IDE
1) Unzip the SparkApplication.zip file
2) Import to the IDE as Maven Project 
3) Add the following argument in Run Configuration - 52.55.237.11:9092 123 stockData
4) Run!
