package classifier;

import java.io.IOException;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainClass {

	public static void main(String[] args) throws IOException {
		
		SparkSession spark = SparkSession.builder()/*.master("local")*/.appName("RF Classifier").getOrCreate();

		// Dataset<Row> data = spark.read().format("libsvm").load("/tmp/out_train.txt");
		Dataset<Row> data = spark.read().format("libsvm").load("/tmp/libsvm_features.dat");

		// Index labels, adding metadata to the label column.
		StringIndexerModel labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")
				.fit(data);

		// Automatically identify categorical features (> 4 distinct values), and index them.
		VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures")
				.setMaxCategories(4).fit(data);

		// Split the data into training and test sets
		Dataset<Row>[] splits = data.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		// Train a RandomForest model
		RandomForestClassifier rf = new RandomForestClassifier().setLabelCol("indexedLabel")
				.setFeaturesCol("indexedFeatures");

		// Convert indexed labels back to original labels.
		IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
				.setLabels(labelIndexer.labels());

		// Chain indexers and forest in a Pipeline
		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { labelIndexer, featureIndexer, rf, labelConverter });

		// Train model. This also runs the indexers.
		PipelineModel model = pipeline.fit(trainingData);

		// Make predictions.
		Dataset<Row> predictions = model.transform(testData);

		// Select example rows to display.
		predictions.select("predictedLabel", "label", "features").show(5);

		// Select (prediction, true label) and compute test error
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test Error = " + (1.0 - accuracy));

		RandomForestClassificationModel rfModel = (RandomForestClassificationModel) (model.stages()[2]);
		// System.out.println("Learned classification forest model:\n" +
		// rfModel.toDebugString());

		/*LabeledPoint lp = new LabeledPoint(102,
				Vectors.dense(125, 31.875, -2.56068, 4.85543, 7.96092, -7.606435, -40.06635, -49.73465, 0, 36.5,
						0.3686075, 6.213635, -0.01960105, -16.4553, -56.7185, 0, 0, 33.9375, 8.87546, -0.02982385,
						0.177018, -62.30185, 126, 31.875, 7.11171, 16.5181, 23.0074, 1.35682, -22.5078, -43.2779, 0,
						36.5625, 1.7867, 10.0081, 0.568527, -8.52391, -51.28, 0, 0, 33.9375, 19.764, 0.906795, 2.00854,
						-52.4191, 124.8867, 31.85242, -2.633314, 4.724483, 8.003088, -7.440402, -39.51099, -50.52429, 0,
						36.50696, 0.3450608, 6.273911, -0.01707619, -16.67193, -56.42863, 0, 0, 33.90955, 8.984526,
						0.02801612, 0.0504861, -61.78451, 0.3632579, 0.03005345, 3.127671, 3.386542, 2.769191, 4.721502,
						5.635178, 3.785863, 0, 0.01967785, 0.6184803, 1.039353, 0.2195448, 4.466452, 2.694632, 0, 0,
						0.0311061, 4.044443, 0.2823475, 1.16212, 3.844146, -0.2459051, 0.5412921, -0.09684879,
						0.5157439, -0.8640202, -0.3031852, 0.02290513, -0.1711173, -0.04348413, 0.05722185, -0.05662553,
						-0.05429346, 0, -0.06812605, 0, -0.8210034, 0.4003918, -0.3857184));
*/
		//OVO Dataset<Row> d = spark.read().format("libsvm").load("D:/out5.txt");
		// StringIndexerModel labelIndexer2 = new
		// StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(d);

		// Automatically identify categorical features, and index them.
		// Set maxCategories so features with > 4 distinct values are treated as
		// continuous.
		// VectorIndexerModel featureIndexer2 = new
		// VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(d);
		//OVO Dataset<Row> pr = model.transform(d);

		//OVA petljafor (Row r : pr.select("label", "predictedLabel").collectAsList()) {
		//	System.out.println("label= " + r.get(0) + ", predictedLabel= " + r.get(1));
		//}
		/*
		 * Iterator<Row> it = pr.toLocalIterator(); int i = 0; while (it.hasNext()) {
		 * Row r = it.next(); if (i % 100 == 0) System.out.println(r); i++; }
		 */
		// System.out.println(pr.);

		/*
		 * System.out.println("Prediction: " +
		 * rfModel.predict(Vectors.dense(125,31.875,-2.56068,4.85543,7.96092,-7.606435,-
		 * 40.06635,-49.73465,0,36.5,0.3686075,6.213635,
		 * -0.01960105,-16.4553,-56.7185,0,0,33.9375,8.87546,-0.02982385,0.177018,-62.
		 * 30185,126,31.875,7.11171,16.5181,23.0074,1.35682,-22.5078,-43.2779,0,
		 * 36.5625,1.7867,10.0081,0.568527,-8.52391,-51.28,0,0,33.9375,19.764,0.906795,2
		 * .00854,-52.4191,124.8867,31.85242,-2.633314,4.724483,8.003088,
		 * -7.440402,-39.51099,-50.52429,0,36.50696,0.3450608,6.273911,-0.01707619,-16.
		 * 67193,-56.42863,0,0,33.90955,8.984526,0.02801612,0.0504861,
		 * -61.78451,0.3632579,0.03005345,3.127671,3.386542,2.769191,4.721502,5.635178,3
		 * .785863,0,0.01967785,0.6184803,1.039353,0.2195448,4.466452,2.694632,0,0,
		 * 0.0311061,4.044443,0.2823475,1.16212,3.844146,-0.2459051,0.5412921,-0.
		 * 09684879,0.5157439,-0.8640202,-0.3031852,0.02290513,-0.1711173,-0.04348413,
		 * 0.05722185,-0.05662553,-0.05429346,0,-0.06812605,0,-0.8210034,0.4003918,-0.
		 * 3857184)));
		 */
		// rfModel.write().overwrite().save("/tmp/spark-output/model_windows");
		// spark.stop();
		
		model.write().overwrite().save("/tmp/pipeline-model");
		rfModel.write().overwrite().save("/tmp/rf-model");
	}
}
