# Feeyo-RedisProxy

一个分布式 Redis 解决方案, 上层应用可以像使用单机的 Redis 一样使用, RedisProxy 底层会处理请求的分发，一切对你无感 ～

内部的服务能力，当前单节点RedisProxy，日处理查询量50亿+， QPS维持在( 50K～80K )

## 目前已实现的特性
*  支持 redis standalone 、 redis cluster、kafka cluster 代理
*  支持 redis cluster 情况下的跨片 marge 操作，如：pipeline 、 mget、 mset、 del mulit key 指令， 通过虚拟内存，解决高并发情况下的 pipeline 的数据堆积问题
*  支持 多租户、只读权限、自动分配db、自动SET过期时间
*  支持 非法字符key的拦截
*  支持 访问的数据监控、 use 指令，实时切换后端代理
*  支持 用户、代理配置的热更新 等等 ...

 <a href="resources/docs/RedisProxy&KafkaProxy.pdf" rel="nofollow"> 使用手册 </a>

## 关于 Architecture 
![img](resources/docs/images/3d5201396f213cf78c61c68c2e8c77a5.png)

### 1.9 版本开始，我们支持通过 Redis 协议，对外提供 kafka 队列服务

	1、kafka 配置, 参见 kafka.xml
	2、kafka 指令描述
		KPUSH {topic}{content}
		KPUSH {topic}{partition}{content}
		KPOP {topic}
		KPOP {topic}{partition}{offset}
		KPARTITIONS {topic}	    	 				
		KOFFSET {topic}{partition}{time}	    	 				  
	
		注：KPOP 不指定 consume offset， 我们自动管理， 指定点位的，我们不管理，也不更新，
		   需要 client 自己维持 consume offset
	3、kafka 管理指令
	 使用 kafka 的队列功能，我们需要提前分配，然后初始化启动 或 通过管理账户的 reload kafka 指令进行热加载，
	 加载的过程中，我们会检测 topic， 且会自动的新增到后端的 kafka 中


### 扩展的 Redis 运维指令
	
	 动态切换集群
	 USE {poodId}
	 
	 配置热更新
	 RELOAD USER
	 RELOAD ALL
	 RELOAD FRONT
	 
	 运行信息
	 SHOW USER
	 SHOW USER_NET_IO 
	 SHOW USER_CMD
	 SHOW USER_CMD_DETAIL {password}
	 
	 SHOW CPU
	 SHOW MEM
	 SHOW QPS
	 SHOW CONN
	 SHOW USER_CONN
	 SHOW BUFFER
	 SHOW BIGKEY
	 SHOW BIGLENGTH
	 SHOW SLOWKEY
	 SHOW CMD
	 SHOW VER
	 SHOW NET_IO
	 SHOW VM
	 SHOW POOL
	 SHOW COST
	 
	 日志信息
	 SHOW LOG_ERROR {line}
	 SHOW LOG_WARN {line}
	 SHOW LOG_INFO {line}
	 SHOW LOG_DEBUG {line}
	 
	 JVM 指令依赖 JAVA_HOME 
	 JVM JSTACK
	 JVM JSTAT
	 JVM JMAP_HISTO
	 JVM JMAP_HEAP
	 JVM PS

### 配置

server.xml, 网络及堆外内存池的相关配置
  
| 节点        | 属性      	 | 属性值 		  	 |  描述 						 		|
| :--------  | :----------   | :-------------    | :---------------------------  		|
| property   | name	    	 | port  		  	 |  服务端口号      				 		|
| property   | name	    	 | reactorSize    	 |  reactor 用于调度nio，设置为内核大小即可  	|
| property   | name	    	 | maxBufferSize  	 |  堆外内存，可利用的最大空间      			|
| property   | name	    	 | minBufferSize  	 |  堆外内存，可利用的最小空间，初始化就会建立   |
| property   | name	    	 | minChunkSize   	 |  最小的 chunk      					|
| property   | name	    	 | increment  	  	 |  minChunkSize 到 maxChunkSize 步长		|
| property   | name	    	 | maxChunkSize	     |  最大的 chunk       					|
| property   | name	    	 | bossSize  	  	 |  工作任务的线程数      					|
| property   | name	    	 | timerSize  	  	 |  定时任务的线程数      			    	|
| property   | name	    	 | frontIdleTimeout	 |  前端连接闲置后的超时时间（毫秒）	      	|
| property   | name	    	 | backendIdleTimeout|  后端连接闲置后的超时时间（毫秒）  			|
	

pool.xml, 连接池配置信息
	
| 节点        | 属性    			  |  描述 |
| :--------  | :----------------  | :---------------------------------------------------------- |
| pool       | id      			  |   唯一编号    												|
| pool       | name     		  |   名称    													|
| pool       | type    			  |   类型，0 表示 redis standalone， 1 表示redis cluster    		|
| pool       | maxCon   		  |   最大的连接数，超过拒绝透传    									|
| pool       | minCon   		  |   最小的连接数   										 		|
| node       | ip      			  |   实际redis node ip   										|
| node       | port     		  |   实际redis node port    									|

  
user.xml, 用户配置信息

| 节点        | 属性    	|  描述 |
| :--------  | :-----   | :-------------------------------------------- |
| user       | password |   auth 登录密码    								|
| user       | poolId   |   通过该id 连接后端连接池    						|
| user       | prefix  	|   自动前缀  									|
| user       | selectDb |   非集群情况下，支持强制读写的 redis db， 默认0    	|
| user       | maxCon   |   该用户的最大连接数，默认800   					|
| user       | readonly |   是否只读账户，默认非只读账户   					|
| user       | isAdmin  |   是否管理员，默认非管理员   						|
  
### 运行
	run.sh start
	run.sh stop
	run.sh restart
	
	