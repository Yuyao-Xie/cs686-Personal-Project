package edu.usfca.dataflow;

import org.junit.Rule;
import org.junit.rules.Timeout;

public class __TestBase {
  // Note: This rule is used to stop the grading system from going down when your code gets stuck (safety measure).
  // If you think this prevents your submission from getting graded normally, ask on Piazza.
  @Rule
  public Timeout timeout = Timeout.millis(2000);

  public static final String UUID1 = "3721afb5-22de-408b-b97d-cb6ca8953cc9";
  public static final String UUID2 = "b586eaed-6788-4e5e-9084-268170604108";
  public static final String UUID3 = "24615bd7-f4f1-4ada-8873-d4bb68391677";
  public static final String UUID4 = "aaa80bc1-a995-4fdf-8352-6cd3ecd17c74";
  public static final String UUID5 = "af7d508e-b18f-49b7-b1b3-12f95b8ae168";

  public static final String Bundle1 = "edu.usfca.cs.app1";
  public static final String Bundle2 = "edu.usfca.cs.app2";
  public static final String Bundle3 = "id12341234";
  public static final String Bundle4 = "id98769876";
  public static final String[] Bundles = new String[] {Bundle1, Bundle2, Bundle3, Bundle4};
}
