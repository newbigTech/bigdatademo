package hadoop;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: haibo
 * Date: 2018/1/15 下午6:29
 * Desc:
 */
public class Mainddd {
    public static void main(String[] args) {
        Map<String, AtomicLong> msa = Maps.newConcurrentMap();
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        msa.put("a", new AtomicLong(1));
        for (Long i = 0L; i < 70000000l; i++) {
            msa.get("a").getAndIncrement();
        }
        System.out.println(msa.get("a").get());
        System.out.println(stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
    }
}
