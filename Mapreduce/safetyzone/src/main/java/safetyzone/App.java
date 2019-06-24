package safetyzone;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args) throws IOException, TasteException{
        
	      File userPreferencesFile = new File("/home/bhavik/Desktop/bigdata/crimes_recommendation.csv");
	      DataModel dataModel = new FileDataModel(userPreferencesFile);
	      
	      UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(dataModel);
	      UserNeighborhood userNeighborhood = new ThresholdUserNeighborhood(0.1, userSimilarity, dataModel);
	 
	      // Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
	      Recommender genericRecommender =  new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);
	 
	      // Recommend 5 items for each user
	      for (LongPrimitiveIterator iterator = dataModel.getUserIDs(); iterator.hasNext();)
	      {
	          long district = iterator.nextLong();
	 
	          // Generate a list of 5 recommendations for the user
	          List<RecommendedItem> itemRecommendations = genericRecommender.recommend(district, 5);
	 
	          System.out.format("district Id: %d%n", district);
	 
	          if (itemRecommendations.isEmpty())
	          {
	              System.out.println("safest district.");
	          }
	          else
	          {
	              // Display the list of recommendations
	              for (RecommendedItem recommendedItem : itemRecommendations)
	              {
	                  System.out.format("Chances of crime committed %d. Strength of the preference: %f%n", recommendedItem.getItemID(), recommendedItem.getValue());
	              }
	          }
	      }
	    }
}
