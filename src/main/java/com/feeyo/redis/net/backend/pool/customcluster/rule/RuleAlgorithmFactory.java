package com.feeyo.redis.net.backend.pool.customcluster.rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * custom cluster rule
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class RuleAlgorithmFactory {
    private static Map<String, RuleAlgorithm> ruleAlgorithmMap;

    static {
        Map<String, RuleAlgorithm> map = new HashMap<>();
        map.put("suffix_hint", new SuffixHintRule());

        ruleAlgorithmMap = Collections.unmodifiableMap(map);
    }

    public static RuleAlgorithm getRuleAlgorithm(String name) {
        RuleAlgorithm rule = ruleAlgorithmMap.get(name);
        return rule;
    }
}
