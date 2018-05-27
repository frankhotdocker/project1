package ratpack.example.java;

import ratpack.handling.Context;

/**
 * An example service interface.
 *
 * @see MyHandler
 */
public interface MyService {

    String getValue(Context context);

}
