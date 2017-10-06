package com.feeyo.redis.net.backend.pool.customcluster.rule;

import com.feeyo.redis.engine.codec.RedisRequest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * #\d+# suffix indicates the node index
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class SuffixHintRule implements RuleAlgorithm {
    private static Pattern pattern = Pattern.compile(".*#\\d+#$");

    @Override
    public int calculate(RedisRequest request) { // request already rewrite
        byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
        if (requestKey != null) {
            String key = new String(requestKey);
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches()) {
                String[] strs = key.split("#");
                int idx = Integer.parseInt(strs[strs.length - 1]);
                request.getArgs()[1] = restoreKey(key).getBytes();

                return idx;
            }
        }

        return 0;
    }

    private String restoreKey(String key) {
        return key.substring(0,key.indexOf('#'));
    }
}
