package com.dalong.udf;

import nl.basjes.parse.useragent.UserAgentAnalyzer;

public class UserAgentAnalyzerProvider {
    public static UserAgentAnalyzer getInstance() {
        return UserAgentAnalyzerHolder.INSTANCE;
    }

    private static class UserAgentAnalyzerHolder {
        private static final UserAgentAnalyzer INSTANCE = UserAgentAnalyzer.newBuilder()
                .dropTests()
                .hideMatcherLoadStats()
                .immediateInitialization()
                .build();
    }
}
