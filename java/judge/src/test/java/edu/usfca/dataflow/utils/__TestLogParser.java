package edu.usfca.dataflow.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Event.PurchaseEvent;
import edu.usfca.protobuf.Event.PurchaseEvent.Store;

public class __TestLogParser {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // The timeout provided below should be more than sufficient (yet, if it turns out it's too short, I'll adjust it and
  // re-grade so you don't have to worry about that).
  // You can disable this for your local tests, though (just remove the following two lines).
  @Rule
  public Timeout timeout = Timeout.millis(5000);

  // Try these on json formatter website(s), like https://jsonformatter.curiousconcept.com/
  static final String[] SAMPLES = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=390.244&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppleStore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"54.148.98.94\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=1105.691&event_type=PURcHASE&event_id=XfCS1xEKfJpgVss0Kxe&decoded=wow%21&store=aPpstore&gps_adid=407cc9c9-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=DChu2S719\"},\"insertId\":\"lSpRhcyyvb\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:48.194991Z\",\"spanId\":\"F2ChTShB\",\"trace\":\"projects/beer-spear/traces/mR6KF2Y0o2r96lwmjyhL\"}",
      "{\"httpRequest\":{\"remoteIp\":\"81.153.19.87\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?ios_idfa=F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&amount=1869.919&event_type=PURChaSE&event_id=Xw3rY5&store=playStore&bundle=F7GEqu1ZIlMthlv25rjvimSc\"},\"insertId\":\"WHTy7S3p8\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:53:59.715888Z\",\"spanId\":\"wiYcjQj5BqFG\",\"trace\":\"projects/beer-spear/traces/IkUh223zqzTFjms\"}"};

  static final String[] INVALID_SAMPLES = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=390.244&event_type=ggwp&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=390.244&event_type=PURChAsE&event_id=&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":403,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=390.244&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}"};

  @Test
  public void testSample01() {
    KV<DeviceId, PurchaseEvent> kv = LogParser.getIdAndPurchaseEvent(SAMPLES[0]);
    assertEquals(OsType.ANDROID, kv.getKey().getOs());
    assertTrue("06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx".equalsIgnoreCase(kv.getKey().getUuid()));
    assertEquals(390, kv.getValue().getAmount());
    assertEquals("iIHl65T0o", kv.getValue().getAppBundle());
    assertEquals(Store.UNKNOWN_STORE, kv.getValue().getStore());
  }

  @Test
  public void testSample02() {
    KV<DeviceId, PurchaseEvent> kv = LogParser.getIdAndPurchaseEvent(SAMPLES[1]);
    assertEquals(OsType.ANDROID, kv.getKey().getOs());
    assertTrue("407cc9c9-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx".equalsIgnoreCase(kv.getKey().getUuid()));
    assertEquals(1106, kv.getValue().getAmount());
    assertEquals("DChu2S719", kv.getValue().getAppBundle());
    assertEquals(Store.APPSTORE, kv.getValue().getStore());
  }

  @Test
  public void testSample03() {
    KV<DeviceId, PurchaseEvent> kv = LogParser.getIdAndPurchaseEvent(SAMPLES[2]);
    assertEquals(OsType.IOS, kv.getKey().getOs());
    assertTrue("F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx".equalsIgnoreCase(kv.getKey().getUuid()));
    assertEquals(1870, kv.getValue().getAmount());
    assertEquals("F7GEqu1ZIlMthlv25rjvimSc", kv.getValue().getAppBundle());
    assertEquals(Store.PLAYSTORE, kv.getValue().getStore());
  }

  @Test
  public void testInvalidSamples() {
    for (String invalid : INVALID_SAMPLES) {
      assertNull(LogParser.getIdAndPurchaseEvent(invalid));
    }
  }
}
