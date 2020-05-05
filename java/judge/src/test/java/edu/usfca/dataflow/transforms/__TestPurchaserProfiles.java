package edu.usfca.dataflow.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import edu.usfca.protobuf.BusinessOuterClass;
import edu.usfca.protobuf.CategoryOuterClass;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.ReviewOuter;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Iterables;

import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;

public class __TestPurchaserProfiles {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // The timeout provided below should be more than sufficient (yet, if it turns out it's too short, I'll adjust it and
  // re-grade so you don't have to worry about that).
  // You can disable this for your local tests, though (just remove the following two lines).
  // @Rule
  // public Timeout timeout = Timeout.millis(10000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // Try these on json formatter website(s), like https://jsonformatter.curiousconcept.com/
  // These are taken from sample-tiny.txt in the judge/resources directory.
  // same files can also be found under dataflow/resources directory, for your convenience.
  // Disclaimer: remoteIp and other fields are randomly generated, and uuids also anonymized.

  static final String[] SAMPLES = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=390.244&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"54.148.98.94\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=1105.691&event_type=PURcHASE&event_id=XfCS1xEKfJpgVss0Kxe&decoded=wow%21&store=aPpstore&gps_adid=407cc9c9-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=DChu2S719\"},\"insertId\":\"lSpRhcyyvb\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:48.194991Z\",\"spanId\":\"F2ChTShB\",\"trace\":\"projects/beer-spear/traces/mR6KF2Y0o2r96lwmjyhL\"}",
      "{\"httpRequest\":{\"remoteIp\":\"81.153.19.87\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?ios_idfa=F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&amount=1869.919&event_type=PURChaSE&event_id=Xw3rY5&store=AppsTorE&bundle=F7GEqu1ZIlMthlv25rjvimSc\"},\"insertId\":\"WHTy7S3p8\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:53:59.715888Z\",\"spanId\":\"wiYcjQj5BqFG\",\"trace\":\"projects/beer-spear/traces/IkUh223zqzTFjms\"}"};

  // Almost identical to SAMPLES, with minor changes.
  static final String[] SAMPLES2 = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=1390.444&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"54.148.98.94\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=2105.991&event_type=PURcHASE&event_id=XfCS1xEKfJpgVss0Kxe&decoded=wow%21&store=aPpstore&gps_adid=407cc9c9-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=DChu2S719\"},\"insertId\":\"lSpRhcyyvb\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:48.194991Z\",\"spanId\":\"F2ChTShB\",\"trace\":\"projects/beer-spear/traces/mR6KF2Y0o2r96lwmjyhL\"}",
      "{\"httpRequest\":{\"remoteIp\":\"81.153.19.87\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?ios_idfa=F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&amount=2869.019&event_type=PURChaSE&event_id=Xw3rY5&store=AppsTorE&bundle=F7GEqu1ZIlMthlv25rjvimSc\"},\"insertId\":\"WHTy7S3p8\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:53:59.715888Z\",\"spanId\":\"wiYcjQj5BqFG\",\"trace\":\"projects/beer-spear/traces/IkUh223zqzTFjms\"}"};

  static String b1 = "{\"business_id\":\"123\",\"name\":\"Arizona Biltmore Golf Club\",\"address\":\"2818 E Camino Acequia Drive\",\"city\":\"Phoenix\",\"state\":\"NV\",\"postal_code\":\"85016\",\"latitude\":33.5221425,\"longitude\":-112.0184807,\"stars\":3.0,\"review_count\":5,\"is_open\":0,\"attributes\":{\"GoodForKids\":\"False\"},\"categories\":\"Golf, Active Life, Shopping\",\"hours\":null}";
  static String b2 = "{\"business_id\":\"abc\",\"name\":\"Supercuts\",\"address\":\"4545 E Tropicana Rd Ste 8, Tropicana\",\"city\":\"Las Vegas\",\"state\":\"NV\",\"postal_code\":\"89121\",\"latitude\":36.099872,\"longitude\":-115.074574,\"stars\":3.5,\"review_count\":3,\"is_open\":1,\"attributes\":{\"RestaurantsPriceRange2\":\"3\",\"GoodForKids\":\"True\",\"BusinessAcceptsCreditCards\":\"True\",\"ByAppointmentOnly\":\"False\",\"BikeParking\":\"False\"},\"categories\":\"Hair Salons, Cosmetics & Beauty Supply, Shopping, Beauty & Spas\",\"hours\":{\"Monday\":\"10:0-19:0\",\"Tuesday\":\"10:0-19:0\",\"Wednesday\":\"10:0-19:0\",\"Thursday\":\"10:0-19:0\",\"Friday\":\"10:0-19:0\",\"Saturday\":\"10:0-19:0\",\"Sunday\":\"10:0-18:0\"}}";
  static String b3 = "{\"business_id\":\",./\",\"name\":\"Edgeworxx Studio\",\"address\":\"20 Douglas Woods Drive Southeast\",\"city\":\"Calgary\",\"state\":\"AB\",\"postal_code\":\"T2Z 1K4\",\"latitude\":50.9436456,\"longitude\":-114.0018283,\"stars\":3.5,\"review_count\":7,\"is_open\":1,\"attributes\":{\"RestaurantsPriceRange2\":\"2\",\"BusinessParking\":\"{'garage': False, 'street': False, 'validated': False, 'lot': True, 'valet': False}\",\"ByAppointmentOnly\":\"True\"},\"hours\":null}";
  static final String[] BSAMPLES = new String[]{b1, b2, b3};

  static String r1 = "{\"review_id\":\"yi0R0Ugj_xUx_Nek0-_Qig\",\"user_id\":\"dacAIZ6fTM6mqwW5uxkskg\",\"business_id\":\"123\",\"stars\":5.0,\"useful\":0,\"funny\":0,\"cool\":0,\"text\":\"Went in for a lunch. Steak sandwich was delicious, and the Caesar salad had an absolutely delicious dressing, with a perfect amount of dressing, and distributed perfectly across each leaf. I know I'm going on about the salad ... But it was perfect.\\n\\nDrink prices were pretty good.\\n\\nThe Server, Dawn, was friendly and accommodating. Very happy with her.\\n\\nIn summation, a great pub experience. Would go again!\",\"date\":\"2018-01-09 20:56:38\"}";
  static String r2 = "{\"review_id\":\"2TzJjDVDEuAW6MR5Vuc1ug\",\"user_id\":\"n6-Gk65cPZL6Uz8qRm3NYw\",\"business_id\":\"abc\",\"stars\":5.0,\"useful\":3,\"funny\":0,\"cool\":0,\"text\":\"I have to say that this office really has it together, they are so organized and friendly!  Dr. J. Phillipp is a great dentist, very friendly and professional.  The dental assistants that helped in my procedure were amazing, Jewel and Bailey helped me to feel comfortable!  I don't have dental insurance, but they have this insurance through their office you can purchase for $80 something a year and this gave me 25% off all of my dental work, plus they helped me get signed up for care credit which I knew nothing about before this visit!  I highly recommend this office for the nice synergy the whole office has!\",\"date\":\"2016-11-09 20:09:03\"}";
  static String r3 = "{\"review_id\":\"Q1sbwvVQXV2734tPgoKj4Q\",\"user_id\":\"hG7b0MtEbXx5QzbzE6C_VA\",\"business_id\":\"abc\",\"stars\":1.0,\"useful\":6,\"funny\":1,\"cool\":0,\"text\":\"Total bill for this horrible service? Over $8Gs. These crooks actually had the nerve to charge us $69 for 3 pills. I checked online the pills can be had for 19 cents EACH! Avoid Hospital ERs at all costs.\",\"date\":\"2013-05-07 04:34:36\"}";
  static final String[] RSAMPLES = new String[]{r1,r2,r3};

  @Test
  public void testSamples01() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(BSAMPLES[0], BSAMPLES[2], BSAMPLES[1])));

    PCollection<BusinessOuterClass.Business> purchasers = inputLogs.apply(new Business.GetBusinessFromJson());

    PAssert.that(purchasers).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (BusinessOuterClass.Business b : out) {

        //assertEquals(0f, b.getStars());

        if (b.getBusinessId().equalsIgnoreCase("123")) {
          found ^= 1 << 0;
          assertEquals("NV", b.getState());
        } else if (b.getBusinessId().equalsIgnoreCase("abc")) {
          found ^= 1 << 1;
          assertEquals("NV", b.getState());
        } else if (b.getBusinessId().equalsIgnoreCase(",./")) {
          found ^= 1 << 2;
          assertEquals("AB", b.getState());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }


  @Test
  public void testSamples02() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(RSAMPLES[0], RSAMPLES[2], RSAMPLES[1])));

    PCollection<ReviewOuter.Review> purchasers = inputLogs.apply(new Review.GetReviewFromJson());

    PAssert.that(purchasers).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (ReviewOuter.Review b : out) {

        //assertEquals(0f, b.getStars());

        if (b.getReviewId().equalsIgnoreCase("yi0R0Ugj_xUx_Nek0-_Qig")) {
          found ^= 1 << 0;
          assertEquals("ikCg8xy5JIg_NGPx-MSIDA", b.getBusinessId());
        } else if (b.getReviewId().equalsIgnoreCase("2TzJjDVDEuAW6MR5Vuc1ug")) {
          found ^= 1 << 1;
          assertEquals("WTqjgwHlXbSFevF32_DJVw", b.getBusinessId());
        } else if (b.getReviewId().equalsIgnoreCase("Q1sbwvVQXV2734tPgoKj4Q")) {
          found ^= 1 << 2;
          assertEquals("ujmEBvifdJM6h6RLv4wQIg", b.getBusinessId());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });
    tp.run();
  }

  @Test
  public void testCountGoodReview(){
    PCollection<ReviewOuter.Review> reviews = tp.apply(Create.of(Arrays.asList(RSAMPLES))).apply(new Review.GetReviewFromJson());
    PCollection<KV<String, Long>> numOfGoodReviews = reviews.apply(new Review.CountGoodReviews(1F));
    PAssert.that(numOfGoodReviews).satisfies(out ->{
      assertEquals(2, Iterables.size(out));
      int found = 0;
      for(KV<String, Long> kv: out){
        if(kv.getKey().equalsIgnoreCase("123")){
          found ^= 1 << 0;
          assertEquals((Long)1L,kv.getValue() );
        }else if(kv.getKey().equalsIgnoreCase("abc")){
          found ^= 1 << 1;
          assertEquals((Long)2L, kv.getValue());
        }else{
          fail();
        }
      }assertEquals((1 << 2) - 1, found);
      return null;
    });
    tp.run();
  }

  @Test
  public void testAVG(){
    PCollection<ReviewOuter.Review> reviews = tp.apply(Create.of(Arrays.asList(RSAMPLES))).apply(new Review.GetReviewFromJson());
    PCollection<KV<String, Float>> AVGStars = reviews.apply(new Review.GetAVGStars());
    PAssert.that(AVGStars).satisfies(out ->{
      assertEquals(2, Iterables.size(out));
      int found = 0;
      for(KV<String, Float> kv: out){
        if(kv.getKey().equalsIgnoreCase("123")){
          found ^= 1 << 0;
          assertEquals((Float) 5F,kv.getValue() );
        }else if(kv.getKey().equalsIgnoreCase("abc")){
          found ^= 1 << 1;
          assertEquals((Float) 3F, kv.getValue());
        }else{
          fail();
        }
      }assertEquals((1 << 2) - 1, found);
      return null;
    });
    tp.run();
  }

  @Test
  public void testHigh() {
    PCollection<ReviewOuter.Review> reviews = tp.apply(Create.of(Arrays.asList(RSAMPLES))).apply(new Review.GetReviewFromJson());
    PCollection<BusinessOuterClass.Business> businesses =
            tp.apply(Create.of(Arrays.asList(BSAMPLES))).apply(new Business.GetBusinessFromJson());
    PCollection<KV<String, Long>> numOfGoodReviews = reviews.apply(new Review.CountGoodReviews(3F));
    PCollection<KV<String, Float>> AVGStars = reviews.apply(new Review.GetAVGStars());
    PCollection<BusinessOuterClass.Business> filtered = Business.ExtractPopularBusiness
            .filterBusiness(businesses,numOfGoodReviews,AVGStars, 3F,1);
    PAssert.that(filtered).satisfies(out ->{
      assertEquals(2,Iterables.size(out));
      return null;
    });
    tp.run();
  }

  @Test
  public void testCategory(){
    PCollection<ReviewOuter.Review> reviews = tp.apply(Create.of(Arrays.asList(RSAMPLES))).apply(new Review.GetReviewFromJson());
    PCollection<BusinessOuterClass.Business> businesses =
            tp.apply(Create.of(Arrays.asList(BSAMPLES))).apply(new Business.GetBusinessFromJson());
    PCollection<KV<String, Long>> numOfGoodReviews = reviews.apply(new Review.CountGoodReviews(3F));
    PCollection<KV<String, Float>> AVGStars = reviews.apply(new Review.GetAVGStars());
    PCollection<BusinessOuterClass.Business> filtered = Business.ExtractPopularBusiness
            .filterBusiness(businesses,numOfGoodReviews,AVGStars, 3F,1);
    PCollection<CategoryOuterClass.Category> cas = filtered.apply(new Category.GetCategoryFromBusiness()).apply(new Category.mergeCategories());
    PAssert.that(cas).satisfies(out ->{
      assertEquals(6,Iterables.size(out));
      return null;
    });
    tp.run();
  }
/*
  @Test
  public void testDaily01() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1])));

    PCollection<PurchaserProfile> merged = inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (PurchaserProfile pp : out) {

        assertEquals(1, pp.getPurchaseTotal());

        if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("f199dfde")) {
          found ^= 1 << 0;
          assertEquals(OsType.IOS, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("407cc9c9")) {
          found ^= 1 << 1;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("06798d72")) {
          found ^= 1 << 2;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testDaily02() {
    PCollection<String> inputLogs =
        tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1], SAMPLES2[0], SAMPLES2[1], SAMPLES2[2])));

    PCollection<PurchaserProfile> merged = inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (PurchaserProfile pp : out) {

        assertEquals(2, pp.getPurchaseTotal());

        if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("f199dfde")) {
          found ^= 1 << 0;
          assertEquals(OsType.IOS, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("407cc9c9")) {
          found ^= 1 << 1;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("06798d72")) {
          found ^= 1 << 2;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testExtract01() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1])));

    PCollection<InAppPurchaseProfile> merged =
        inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles()).apply(new ExtractInAppPurchaseData());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (InAppPurchaseProfile iapp : out) {
        assertEquals(1, iapp.getNumPurchasers());

        if (iapp.getBundle().equalsIgnoreCase("dchu2s719")) {
          found ^= 1 << 0;
          assertEquals(1106, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("iihl65t0o")) {
          found ^= 1 << 1;
          assertEquals(390, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("f7gequ1zilmthlv25rjvimsc")) {
          found ^= 1 << 2;
          assertEquals(1870, iapp.getTotalAmount());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testExtract02() {
    PCollection<String> inputLogs =
        tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1], SAMPLES2[0], SAMPLES2[1], SAMPLES2[2])));

    PCollection<InAppPurchaseProfile> merged =
        inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles()).apply(new ExtractInAppPurchaseData());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (InAppPurchaseProfile iapp : out) {
        assertEquals(1, iapp.getNumPurchasers());

        if (iapp.getBundle().equalsIgnoreCase("dchu2s719")) {
          found ^= 1 << 0;
          assertEquals(2106 + 1106, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("iihl65t0o")) {
          found ^= 1 << 1;
          assertEquals(1390 + 390, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("f7gequ1zilmthlv25rjvimsc")) {
          found ^= 1 << 2;
          assertEquals(2869 + 1870, iapp.getTotalAmount());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }*/
}
