package com.feeyo.kafka.net.backend.runtime;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;

import com.feeyo.kafka.codec.ApiVersionsResponse.ApiVersion;

public class BrokerApiVersion {
	
	private static Map<Short, ApiVersion> apiVersions = null;
	
	public static void setApiVersions(Map<Short, ApiVersion> vers) {
		apiVersions = vers;
	}
	
	public static ApiVersion getApiVersion(short key) {
		if ( apiVersions != null)
			return apiVersions.get(key);
		return null;
	}
	
	public static short getProduceVersion() {
		
		if ( apiVersions == null)
			return -1;
		
		// 现在代码最多支持到5
		short version = 5;
		ApiVersion apiVersion = apiVersions.get(ApiKeys.PRODUCE.id);
		if (apiVersion.maxVersion < version){
			version =  apiVersion.maxVersion;
		} else if (apiVersion.minVersion > version) {
			version = -1;
		}
		
		return version;
	}
	
	public static short getConsumerVersion() {
		
		if ( apiVersions == null)
			return -1;
		
		// 现在代码最多支持到7
		short version = 7;
		ApiVersion apiVersion = apiVersions.get(ApiKeys.FETCH.id);
		if (apiVersion.maxVersion < version) {
			version = apiVersion.maxVersion;
		} else if (apiVersion.minVersion > version) {
			version = -1;
		}
		
		return version;
	}
	
	public static short getListOffsetsVersion() {
		
		if ( apiVersions == null)
			return -1;
		
		// 现在代码最多支持到7
		short version = 2;
		ApiVersion apiVersion = apiVersions.get(ApiKeys.LIST_OFFSETS.id);
		if (apiVersion.maxVersion < version) {
			version = apiVersion.maxVersion;
		} else if (apiVersion.minVersion > version) {
			version = -1;
		}

		return version;
	}
}