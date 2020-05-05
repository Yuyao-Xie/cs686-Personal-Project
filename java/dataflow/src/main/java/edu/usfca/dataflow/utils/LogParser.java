package edu.usfca.dataflow.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

import edu.usfca.protobuf.BusinessOuterClass;
import edu.usfca.protobuf.ReviewOuter;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Event.PurchaseEvent;
import edu.usfca.protobuf.Event.PurchaseEvent.Store;

/**
 * For context & rules, see Lab 05 & Project 03.
 *
 * TODO: If you find it more convenience, you can use your code from Project 3.
 *
 * Otherwise, please use this LogParser class to avoid unexpected results.
 *
 * You can assume that this LogParser class is correct (and the grading system will also use the same LogParser).
 * 
 * Note that UUID is normalized to uppercase in {@link #getDeviceId(Map)} below.
 */
public class LogParser {
  final static JsonParser parser = new JsonParser();
  public static BusinessOuterClass.Business getBusiness(String jsonLogAsLine) {
    //PurchaseEvent.Builder pe = PurchaseEvent.newBuilder();
    BusinessOuterClass.Business.Builder businessBuilder = BusinessOuterClass.Business.newBuilder();
    JsonObject jsonLog = parser.parse(jsonLogAsLine).getAsJsonObject();
    String id = jsonLog.get("business_id").getAsString();
    //List<String> categories = Arrays.asList(jsonLog.get("categories").getAsString().split(","));
    String state = jsonLog.get("state").getAsString();
    float stars = jsonLog.get("stars").getAsFloat();
    List<String> categories = new ArrayList<>();
    if(jsonLog.get("categories") != null && !jsonLog.get("categories").isJsonNull()){
      categories = Arrays.asList(jsonLog.get("categories").getAsString().split(",")).stream().map(String::trim)
              .map(String::toLowerCase).collect(Collectors.toList());
    }else{
      categories.add("Default Category");
    }
    return businessBuilder.setBusinessId(id).addAllCategory(categories).setState(state).setStars(stars).build();
  }

  public static ReviewOuter.Review getReview(String jsonLogAsLine){
    ReviewOuter.Review.Builder reviewBuilder = ReviewOuter.Review.newBuilder();
    JsonObject jsonLog = parser.parse(jsonLogAsLine).getAsJsonObject();
    String business_id = jsonLog.get("business_id").getAsString();
    String review_id = jsonLog.get("review_id").getAsString();
    float stars = jsonLog.get("stars").getAsFloat();
    return reviewBuilder.setReviewId(review_id).setBusinessId(business_id).setStars(stars).build();
  }


  public static KV<DeviceId, PurchaseEvent> getIdAndPurchaseEvent(String jsonLogAsLine) {
    try {
      PurchaseEvent.Builder pe = PurchaseEvent.newBuilder();
      JsonObject jsonLog = parser.parse(jsonLogAsLine).getAsJsonObject();

      // timestamp e.g. "timestamp": "2017-05-01T00:59:58.717127597Z"
      final String timestamp = jsonLog.get("timestamp").getAsString();
      try {
        pe.setEventAt(extractTimestampFromAccessLog(timestamp));
      } catch (Exception e) {
        return null;
      }

      final JsonObject httpReq = jsonLog.getAsJsonObject("httpRequest");
      final String reqUrlString = httpReq.get("requestUrl").getAsString();

      final int status = httpReq.has("status") ? httpReq.get("status").getAsInt() : -999;
      switch (status) {
        case 200: // OK
          break;
        default:
          return null;
      }

      Map<String, String> queryMap;
      try {
        queryMap = getQueryMap(reqUrlString);
        final DeviceId id = getDeviceId(queryMap);
        if (id == null) {
          return null;
        }
        final String bundle = queryMap.getOrDefault("bundle", "");
        if (StringUtils.isBlank(bundle)) {
          return null;
        }
        pe.setAppBundle(bundle);
        final String eventType = queryMap.getOrDefault("event_type", "");
        if (!PURCHASE_EVENTS.contains(eventType.toLowerCase())) {
          return null;
        }

        final String eventId = queryMap.getOrDefault("event_id", "");
        if (StringUtils.isBlank(eventId)) {
          return null;
        }
        pe.setEventId(eventId);
        final String store = queryMap.getOrDefault("store", "unknown_store").toUpperCase();
        try {
          pe.setStore(Store.valueOf(store));
        } catch (Exception e) {
          pe.setStore(Store.UNKNOWN_STORE);
        }

        final String amount = queryMap.getOrDefault("amount", null);
        if (amount == null) {
          return null;
        }
        final int amountInt = Integer.parseInt(String.format("%.0f", Double.parseDouble(amount)));
        pe.setAmount(amountInt);


        return KV.of(id, pe.build());
      } catch (URISyntaxException e) {
        // Terminate on URI syntax exception
        return null;
      }
    } catch (Exception eee) {
      return null;
    }
  }

  private static DeviceId getDeviceId(Map<String, String> queryMap) {
    DeviceId.Builder did = DeviceId.newBuilder();

    final String idfa = queryMap.getOrDefault("ios_idfa", "");
    final String adid = queryMap.getOrDefault("gps_adid", "");
    if (StringUtils.isBlank(idfa) == StringUtils.isBlank(adid)) {
      return null;
    }

    if (!StringUtils.isBlank(idfa)) {
      did.setOs(OsType.IOS).setUuid(idfa.toUpperCase());
    } else {
      did.setOs(OsType.ANDROID).setUuid(adid.toUpperCase());
    }

    return did.build();
  }

  final static Set<String> PURCHASE_EVENTS =
          new ImmutableSet.Builder<String>().add("purchase", "iap", "in-app-purchase").build();

  private static DateTimeFormatter DATE_TIME_NANOS_FORMAT =
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").withZoneUTC();
  public static final DateTimeFormatter DATE_TIME_FORMAT =
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

  // If timestamp string cannot be extracted or there is a parse error, this method will throw an exception.
  public static long extractTimestampFromAccessLog(String timestamp) {
    // e.g. "timestamp": "2017-04-28T10:59:58.907784163Z" or "2017-06-28T10:21:29Z"
    timestamp = timestamp.replace("T", " ").replace("Z", "");
    try {
      return DATE_TIME_NANOS_FORMAT.parseDateTime(timestamp).getMillis();
    } catch (IllegalArgumentException e) {
      return DATE_TIME_FORMAT.parseDateTime(timestamp).getMillis();
    }
  }

  public static Map<String, String> getQueryMap(String reqUrlString) throws URISyntaxException {
    // final String reqUrlStringEncoded = reqUrlString.replace("{", "%7B").replace("}", "%7D");
    final Map<String, String> queryMap = splitQuery(reqUrlString);

    return queryMap;
  }

  public static Map<String, String> splitQuery(String urlString) throws URISyntaxException {
    // While parsing, the encoded URL (hex-encodings, reserved characters, etc) is decoded
    final List<NameValuePair> parsedPairs = URLEncodedUtils.parse(new URI(urlString), Charset.forName("UTF-8"));
    Map<String, String> queryPairs = new LinkedHashMap<>();

    for (NameValuePair nv : parsedPairs) {
      queryPairs.put(nv.getName(), nv.getValue());
    }
    return queryPairs;
  }
}
