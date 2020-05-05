package edu.usfca.dataflow.utils;


import edu.usfca.protobuf.CategoryOuterClass;
import edu.usfca.protobuf.ReviewOuter;
import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.util.*;

public class CombineUtil {
    public static class CombineForAVGStars extends Combine.CombineFn<ReviewOuter.Review, CombineForAVGStars.Accum, Float> {

        private class Accum implements Serializable {
            long numOfReviews = 0L;
            float stars = 0F;
        }

        public Accum createAccumulator() {
            return new Accum();
        }

        public Accum addInput(Accum accum, ReviewOuter.Review p) {
            accum.numOfReviews++;
            accum.stars += p.getStars();
            return accum;
        }

        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum accum = new Accum();
            for(Accum a: accums){
                accum.numOfReviews += a.numOfReviews;
                accum.stars += a.stars;
            }
            return accum;
        }

        public Float extractOutput(Accum accum) {
            return accum.stars/accum.numOfReviews;
        }
    }

    public static class CombineCategory extends Combine.CombineFn<CategoryOuterClass.Category,
            CombineCategory.Accum, Iterable<CategoryOuterClass.Category>> {

        private class Temp implements Serializable{
            String state;
            String title;
            long num;
            long numGoodReviews;
            float star;

            Temp(CategoryOuterClass.Category c){
                this.state = c.getState();
                this.title = c.getTitle();
                this.num = 1L;
                this.numGoodReviews = c.getNumOfGoodReview();
                this.star = c.getAverageStar();
            }

            void update(CategoryOuterClass.Category c){
                this.num += 1L;
                this.numGoodReviews += c.getNumOfGoodReview();
                this.star += c.getAverageStar();
            }

            void update(Temp t){
                this.num += t.num;
                this.numGoodReviews += t.numGoodReviews;
                this.star += t.star;
            }
        }

        private class Accum implements Serializable {
            Map<String, Temp> map= new HashMap<>();
        }

        public Accum createAccumulator() {
            return new Accum();
        }

        public Accum addInput(Accum accum, CategoryOuterClass.Category c) {
            if(accum.map.containsKey(c.getTitle())){
                accum.map.get(c.getTitle()).update(c);
            }else{
                accum.map.put(c.getTitle(), new Temp(c));
            }
            return accum;
        }

        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum accum = new Accum();
            for(Accum a: accums){
                mergeAccum(accum,a);
                System.out.println(a.map.size());
            }
            System.out.println("accumde: " + accum.map.size());
            return accum;
        }

        private void mergeAccum(Accum res, Accum accum){
            for(Map.Entry<String, Temp> entry: accum.map.entrySet()){
                if(res.map.containsKey(entry.getKey())){
                    res.map.get(entry.getKey()).update(entry.getValue());
                }else{
                    res.map.put(entry.getKey(), entry.getValue());
                }
            }
        }

        public Iterable<CategoryOuterClass.Category> extractOutput(Accum accum) {
            List<CategoryOuterClass.Category> res = new ArrayList<>();
            for(Map.Entry<String, Temp> entry: accum.map.entrySet()){
                float avgStar = entry.getValue().star / entry.getValue().num;
                res.add(CategoryOuterClass.Category.newBuilder().setAverageStar(avgStar)
                        .setNumOfGoodReview(entry.getValue().numGoodReviews)
                        .setTitle(entry.getValue().title).setState(entry.getValue().state).build());
            }
            return res;
        }
    }
}
