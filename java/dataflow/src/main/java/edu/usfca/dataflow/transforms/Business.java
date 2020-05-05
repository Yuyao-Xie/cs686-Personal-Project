package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.utils.CombineUtil;
import edu.usfca.dataflow.utils.LogParser;
import edu.usfca.protobuf.BusinessOuterClass;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.HashMap;
import java.util.Map;

public class Business {
    public static class GetBusinessFromJson extends PTransform<PCollection<String>, PCollection<BusinessOuterClass.Business>> {

        @Override
        public PCollection<BusinessOuterClass.Business> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new DoFn<String, BusinessOuterClass.Business>() {
                @ProcessElement
                public void process(@Element String st, OutputReceiver<BusinessOuterClass.Business> out){
                    BusinessOuterClass.Business business = LogParser.getBusiness(st);
                    if(business != null){
                        out.output(business);
                    }
                }
            }));
        }
    }

    /*
    * take <BusinessId, numOfGoodReview>, <BusinessId, AVGstars> as input, extract popular business from businesses
    * */
    public static class ExtractPopularBusiness {

        public static PCollection<BusinessOuterClass.Business> filterBusiness(PCollection<BusinessOuterClass.Business> businesses,
                                                               PCollection<KV<String, Long>> numOfGoodReviews,
                                                               PCollection<KV<String, Float>> AVGStars,
                                                                       float standard_avg_stars, int standard_good_review_count) {
            PCollectionView<Map<String, Long>> goodReviewMap = numOfGoodReviews.apply(View.asMap());
            businesses = businesses.apply(ParDo.of(new DoFn<BusinessOuterClass.Business, BusinessOuterClass.Business>() {
                Map<String, Long> map;
                @ProcessElement
                public void process(ProcessContext c, OutputReceiver<BusinessOuterClass.Business> out){
                    if(map == null){
                        map = new HashMap<>();
                        map.putAll(c.sideInput(goodReviewMap));
                    }
                    if(map.containsKey(c.element().getBusinessId()) ){
                        long count = map.get(c.element().getBusinessId());
                        if(count >= standard_good_review_count) {
                            out.output(c.element().toBuilder().setGoodReviewCount(count).build());
                        }
                    }
                }
            }).withSideInputs(goodReviewMap));
            PCollectionView<Map<String, Float>> starMap = AVGStars.apply(View.asMap());
            return businesses.apply(ParDo.of(new DoFn<BusinessOuterClass.Business, BusinessOuterClass.Business>() {
                Map<String, Float> map;
                @ProcessElement
                public void process(ProcessContext c, OutputReceiver<BusinessOuterClass.Business> out){
                    if(map == null){
                        map = new HashMap<>();
                        map.putAll(c.sideInput(starMap));
                    }
                    if(map.containsKey(c.element().getBusinessId())){
                        float star = map.get(c.element().getBusinessId());
                        if(star >= standard_avg_stars){
                            out.output(c.element().toBuilder().setStars(star).build());
                        }
                    }
                }
            }).withSideInputs(starMap));
        }
    }
}
