# Elastic 敏感数据搜索
> 该项目用于快速检索ElasticSearch数据库中是否存在所设关键字的敏感信息

## 0x01 如何使用
1. 将待检测ip放入`ip`文件中，不需要带端口。
2. 在`keywords`文件中设置搜索关键字。
3. 启动main.py文件，输入`1`同步查询数量和详细信息。
4. （可选）输入`2`查询仅查询数量；可根据结果手动筛选。
5. （可选）输入`3`根据查询数量结果查询详情。

## 0x02 Todo
- [ ] 添加正则匹配敏感信息
- [ ] 优化多线程
- [ ] ...