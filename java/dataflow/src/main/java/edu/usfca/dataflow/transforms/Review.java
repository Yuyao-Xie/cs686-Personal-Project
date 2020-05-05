package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.utils.CombineUtil;
import edu.usfca.dataflow.utils.LogParser;
import edu.usfca.protobuf.ReviewOuter;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Review {

    public static class GetReviewFromJson extends PTransform<PCollection<String>, PCollection<edu.usfca.protobuf.ReviewOuter.Review>> {

        @Override
        public PCollection<ReviewOuter.Review> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new DoFn<String, ReviewOuter.Review>() {
                @ProcessElement
                public void process(@Element String st, OutputReceiver<edu.usfca.protobuf.ReviewOuter.Review> out){
                    edu.usfca.protobuf.ReviewOuter.Review review = LogParser.getReview(st);
                    if(review != null){
                        out.output(review);
                    }
                }
            }));
        }
    }

    public static class CountGoodReviews extends PTransform<PCollection<ReviewOuter.Review>, PCollection<KV<String, Long>>>{

        float standard_good_review_stars;

        public CountGoodReviews(float standard_good_review_stars){
            this.standard_good_review_stars = standard_good_review_stars;
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<ReviewOuter.Review> input){
            return input.apply(Filter.by(new SerializableFunction<ReviewOuter.Review, Boolean>() {
                @Override
                public Boolean apply(ReviewOuter.Review input) {
                    return input.getStars() >= standard_good_review_stars;
                }
            }))//get all reviews with good review
                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                    TypeDescriptor.of(ReviewOuter.Review.class)))
                    .via((ReviewOuter.Review r) -> KV.of(r.getBusinessId(),r)))//turn into <businessID, goodReviews>
                    .apply(Count.perKey());
        }
    }

    public static class GetAVGStars extends PTransform<PCollection<ReviewOuter.Review>, PCollection<KV<String, Float>>>{

        @Override
        public PCollection<KV<String, Float>> expand(PCollection<ReviewOuter.Review> input){
            return input.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                    TypeDescriptor.of(ReviewOuter.Review.class)))
                    .via((ReviewOuter.Review r) -> KV.of(r.getBusinessId(),r)))//turn into <businessID, goodReviews>
                    .apply(Combine.perKey(new CombineUtil.CombineForAVGStars()));
        }
    }

}
