import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class MainClass {
	
	private static String topic = "data.stream";
	private static String broker = "35.241.226.218:9092";
	
	private static StructType schema = new StructType().add("label", "int").add("maxHR", "int")
			.add("maxHtemp", "float").add("maxCtemp", "float").add("maxAtemp", "float")
			.add("corrxyHandAcc", "double").add("corryzHandAcc", "double")
			.add("corrxzHandAcc", "double").add("corrxyHandMag", "double").add("corryzHandMag", "double")
			.add("corrxzHandMag", "double").add("corrxyChestAcc", "double").add("corryzChestAcc", "double")
			.add("corrxzChestAcc", "double").add("corrxyChestMag", "double").add("corryzChestMag", "double")
			.add("corrxzChestMag", "double").add("corrxyAnkleAcc", "double").add("corryzAnkleAcc", "double")
			.add("corrxzAnkleAcc", "double").add("corrxyAnkleMag", "double").add("corryzAnkleMag", "double")
			.add("corrxzAnkleMag", "double");
	
	public static void main(String[] args) throws StreamingQueryException
	{
		SparkSession spark = SparkSession.builder().master("local").appName("RF Classifier").getOrCreate();

		Dataset<Row> df = spark
				  .read()
				  .format("kafka")
				  .option("kafka.bootstrap.servers", broker)
				  .option("subscribe", topic)
				  .option("group.id", "KA")
				  .load();
		
		//df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		
		df.show();
		
		spark.streams().awaitAnyTermination();
		//df.createOrReplaceTempView("inputData");
		
		//Dataset<Row> timestamps = spark.sql("select timeStamp from input");
		//int startTimestamp = timestamps.head().getString(0);

		//probaj sa window umesto ovoga
		//i mozda da se ucitava kao javardd<row> umesto kao df
		//Dataset<Row> subset = spark.sql("select * from df where timestamp >= startTimestamp and timeStamp < endTimestamp");
	}
}
