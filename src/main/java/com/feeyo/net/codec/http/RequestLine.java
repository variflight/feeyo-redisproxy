package com.feeyo.net.codec.http;

import java.io.Serializable;

import com.feeyo.net.codec.http.util.Args;

public class RequestLine implements Serializable {

    private static final long serialVersionUID = 2810581718468737193L;

    private final String protocolVer;
    private final String method;
    private final String uri;

    public RequestLine(final String method,
                            final String uri,
                            final String version) {
        super();
        this.method = Args.notNull(method, "Method");
        this.uri = Args.notNull(uri, "URI");
        this.protocolVer = Args.notNull(version, "Http Version");
    }

    public String getMethod() {
        return this.method;
    }

    public String getProtocolVersion() {
        return this.protocolVer;
    }

    public String getUri() {
        return this.uri;
    }


}
