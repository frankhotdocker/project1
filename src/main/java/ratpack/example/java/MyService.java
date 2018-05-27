package ratpack.example.java;

import ratpack.exec.Promise;
import ratpack.handling.Context;

/**
 * An example service interface.
 *
 * @see MyHandler
 */
public interface MyService {

    Promise<String> getValue(Context context);

}
