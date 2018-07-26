# flume-databaseSink
quick to connect to a database as sink

## 本开源项目想干什么？
flune官方的sink组件并没有mysql或者其他数据库的sink组件，只有流向kafka，hadoop等地方的sink。而本人开发的项目刚好需要直接把数据简单过滤后直接流向mysql，且经过基本测试和优化本项目已经基本通用使用，通过简单配置，就可以把指定数据存入mysql。未来将会接入其他类型数据库

## 文档、代码等有疑问的地方欢迎联系：mym0806@163.com，或提出issues，不胜感激

## 开发者文档已经上传：databaseSink.pdf

## wiki说明已更新

## 可运行版本已经发布，功能说明：
+ 支持insert操作全配置式
+ 支持快速扩展
+ 使用apache dpcp连接池
+ 暂时支持mysql
+ 支持负载均衡


write by mym!
