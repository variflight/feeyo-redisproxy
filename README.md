# Feeyo-RedisProxy

## 目前已实现的特性：
*  支持 redis standalone 、 redis cluster 代理
*  支持 redis cluster 情况下的 pipeline 、 mget、 mset、 del mulit key 指令
*  支持 redis pipeline 情况的的数据堆积
*  支持 多租户、只读权限、自动分配db 
*  支持 访问的数据监控、 use 指令，实时切换后端代理
*  支持 用户、代理配置的热更新 等等 ...

### 配置说明

- server.xml
  - socket 设置
  - 堆外内存设置

- pool.xml
  - 代理配置
  
- user.xml
  - 用户配置信息
