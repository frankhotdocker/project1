package ratpack.example.java;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ratpack.test.MainClassApplicationUnderTest;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SiteTest {

  String lineSeparator = System.getProperty("line.separator");

  MainClassApplicationUnderTest aut = new MainClassApplicationUnderTest(MyApp.class);

  @After
  public void tearDown() {
    aut.close();
  }

  @Before
  public void tearUp() {
    System.setProperty("CASSANDRA_SEED", "172.29.0.51");

  }

  @Test
  public void barHandler() {
    assertEquals("from the bar handler", get("bar"));
  }

  @Test
  public void nestedHandler() {
    assertEquals("from the nested handler, var1: x, var2: null", get("nested/x"));
    assertEquals("from the nested handler, var1: x, var2: y", get("nested/x/y"));
  }

  @Test
  @Ignore
  public void cassandraHandler() {
    assertEquals("service value: service-value", get("cassandra"));
  }

  @Test
  public void staticHandler() {
    assertEquals("text asset\n", get("static/images/test.txt"));
  }

  @Test
  public void rootHandler() {
    assertEquals("root handler!", get(""));
    assertEquals("root handler!", get("unknown-path"));
  }

  private String get(String path) {
    return aut.getHttpClient().getText(path);
  }

}
