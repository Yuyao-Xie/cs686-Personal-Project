package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.utils.CombineUtil;
import edu.usfca.dataflow.utils.LogParser;
import edu.usfca.protobuf.BusinessOuterClass;
import edu.usfca.protobuf.CategoryOuterClass;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Category {

    public static class GetCategoryFromBusiness extends PTransform<PCollection<BusinessOuterClass.Business>,
            PCollection<CategoryOuterClass.Category>> {

        @Override
        public PCollection<CategoryOuterClass.Category> expand(PCollection<BusinessOuterClass.Business> input) {
            return input.apply(ParDo.of(new DoFn<BusinessOuterClass.Business, CategoryOuterClass.Category>() {
                @ProcessElement
                public void process(@Element BusinessOuterClass.Business b, OutputReceiver<CategoryOuterClass.Category> out){
                    for(String c: b.getCategoryList()){
                        out.output(CategoryOuterClass.Category.newBuilder().setAverageStar(b.getStars())
                        .setNumOfGoodReview(b.getGoodReviewCount()).setState(b.getState()).setTitle(c).build());
                    }
                }
            }));
        }
    }

    public static class mergeCategories extends PTransform<PCollection<CategoryOuterClass.Category>,
            PCollection<CategoryOuterClass.Category>>{

        @Override
        public PCollection<CategoryOuterClass.Category> expand(PCollection<CategoryOuterClass.Category> input){
            PCollection<CategoryOuterClass.Category> out = input.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(CategoryOuterClass.Category.class)))
            .via((CategoryOuterClass.Category c) -> KV.of(c.getState(),c)))
                    .apply(Combine.perKey(new CombineUtil.CombineCategory()))
                    .apply(ParDo.of(new DoFn<KV<String, Iterable<CategoryOuterClass.Category>>, KV<String, Iterable<CategoryOuterClass.Category>>>() {
                        @ProcessElement
                        public void process( ProcessContext c){
                            c.output(c.element());
                        }
                    }))
                    .apply(Values.create())
                    .apply(ParDo.of(new DoFn<Iterable<CategoryOuterClass.Category>, CategoryOuterClass.Category>() {
                        @ProcessElement
                        public void process(@Element Iterable<CategoryOuterClass.Category> input, OutputReceiver<CategoryOuterClass.Category> out){
                            for(CategoryOuterClass.Category c: input){
                                out.output(c);
                            }
                        }
                    }));
            return out;
        }
    }
}
