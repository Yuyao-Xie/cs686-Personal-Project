package edu.usfca.dataflow.job;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.Main;
import edu.usfca.dataflow.MyOptions;
import edu.usfca.dataflow.transforms.Business;
import edu.usfca.dataflow.transforms.Category;
import edu.usfca.dataflow.transforms.Review;
import edu.usfca.dataflow.utils.PathConfigs;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.BusinessOuterClass;
import edu.usfca.protobuf.CategoryOuterClass;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.ReviewOuter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class YelpJob {

    final static List<TableFieldSchema> BQ_FIELDS = Arrays.asList(//
            new TableFieldSchema().setName("state").setType("STRING"), //
            new TableFieldSchema().setName("title").setType("STRING"), //
            new TableFieldSchema().setName("num_of_good_reviews").setType("INT64"), //
            new TableFieldSchema().setName("stars").setType("FLOAT"));

    public static void execute(MyOptions options) {
        final PathConfigs config = PathConfigs.of(options);
        Pipeline p = Pipeline.create(options);

        // 1. Read "DeviceProfile" "Suspicious User" and "InAppPurchaseProfile" data.
        // The first two are supposed to be the output of your first pipeline (BidLogJob).
        // The second is provided (= supposed to be the output of your pipeline from project 3/4).
        PCollection<String> businessBase64 = p.apply(TextIO.read().from(config.getReadPathToBusiness()));
        PCollection<String> reviewBase64 = p.apply(TextIO.read().from(config.getReadPathToReview()));
        //PCollection<String> iappBase64 = p.apply(TextIO.read().from(config.getReadPathToIAPP()));

        // 2. Get Business, Review
        PCollection<BusinessOuterClass.Business> business = businessBase64.apply(new Business.GetBusinessFromJson());
        PCollection<ReviewOuter.Review> review = reviewBase64.apply(new Review.GetReviewFromJson());

        // 3. Get business filtered
        PCollection<KV<String, Long>> numOfGoodReviews = review.apply(new Review.CountGoodReviews(options.getStandardOfGoodReview()));
        PCollection<KV<String, Float>> AVGStars = review.apply(new Review.GetAVGStars());
        PCollection<BusinessOuterClass.Business> filtered = Business.ExtractPopularBusiness
                .filterBusiness(business,numOfGoodReviews,AVGStars, options.getStandardOfGoodBusiness(),options.getNumOfGoodReview());

        // 4. Get categories
        PCollection<edu.usfca.protobuf.CategoryOuterClass.Category> cas = filtered.apply(new Category.GetCategoryFromBusiness()).apply(new Category.mergeCategories());

        // 5. Write results to GCS as well as to BigQuery.
        PCollection<String> predictionJson =
                cas.apply(MapElements.into(TypeDescriptors.strings()).via((CategoryOuterClass.Category data) -> {
                    try {
                        return ProtoUtils.getJsonFromMessage(data, true);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    return null;
                }));
        predictionJson.apply(TextIO.write().to(config.getWritePathToPredictionData()).withNumShards(1));

        // 6. Write to BigQuery.
        if (options.getExportToBigQuery()) {
            cas.apply(BigQueryIO.<CategoryOuterClass.Category>write().to(Main.DEST_TABLE)//
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)//
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED) //
                    .withSchema(new TableSchema().setFields(BQ_FIELDS))//
                    .withFormatFunction((SerializableFunction<CategoryOuterClass.Category, TableRow>) input //
                            -> new TableRow().set("state",input.getState()).set("title", input.getTitle())//
                            .set("num_of_good_reviews", input.getNumOfGoodReview()).set("stars", input.getAverageStar())));
        }
        p.run().waitUntilFinish();
    }
}
