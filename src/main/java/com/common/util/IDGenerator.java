package com.common.util;

import java.util.UUID;

/**
 * 数据中主键修改为由Java UUID提供。
 * */
public class IDGenerator {
	public static synchronized String getId(){
		return UUID.randomUUID().toString().replace("-", "");
	}
}
