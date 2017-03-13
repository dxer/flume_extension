[TOC]

# flume_extension
包含一些为了处理flume日志收集的问题，自定义的一些插件

###  LineMaxSizeInterceptor

LineMaxSizeInterceptor是一个用来处理日志数据太长问题的拦截器，可以对要收集的日志进行处理，支持忽略整行和日志对应字段置空处理

```
# 忽略整行数据
a1.sources.r1.interceptors=i1
a1.sources.r1.interceptors.i1.type=org.dxer.flume.interceptor.LineMaxSizeInterceptor$Builder
# 最大字节数
a1.sources.r1.interceptors.i1.lineMaxSize=2 
# 是否忽略整行
a1.sources.r1.interceptors.i1.ingoreLine=true
# 日志的字符集，默认utf-8 
a1.sources.r1.interceptors.i1.charset=utf-8
```

```
# 忽略一行中某个字段数据
a1.sources.r1.interceptors=i1
a1.sources.r1.interceptors.i1.type=org.dxer.flume.interceptor.LineMaxSizeInterceptor$Builder
# 最大字节数
a1.sources.r1.interceptors.i1.lineMaxSize=2 
# 是否忽略整行
a1.sources.r1.interceptors.i1.ingoreLine=false
# 要忽略字段数据的下标（从0开始，会以“-”填充）
a1.sources.r1.interceptors.i1.emptyColIndexes=0,1
# 日志分隔符
a1.sources.r1.interceptors.i1.lineSeparator=\t
```
