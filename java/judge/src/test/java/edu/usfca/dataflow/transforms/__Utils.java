package edu.usfca.dataflow.transforms;

import org.junit.Rule;
import org.junit.rules.Timeout;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;

import edu.usfca.protobuf.Common.DeviceId;

// Used only for unit tests
public class __Utils {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // The timeout provided below should be more than sufficient (yet, if it turns out it's too short, I'll adjust it and
  // re-grade so you don't have to worry about that).
  // You can disable this for your local tests, though (just remove the following two lines).
  @Rule
  public Timeout timeout = Timeout.millis(2000);

  static Multiset<String> getMultiSet(String... ss) {
    return new ImmutableMultiset.Builder<String>().add(ss).build();
  }

  // Simply normalize the uuid part to uppercase.
  static DeviceId getCanonicalDeviceId(DeviceId id) {
    return id.toBuilder().setUuid(id.getUuid().toUpperCase()).build();
  }
}
