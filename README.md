# Easy Full-Text Search Engine

## 新特性：

1. 支持基于wiki文档构建倒排索引
2. 索引结构支持Hashtable与Btree
3. 引擎支持全量索引+增量索引，增量索引是基于Hashtable在内存中构建的，支持实时更新，定时合并到全量索引；且支持了DoubleBuffer更新，提升了查询性能；
4. 全量索引分为SmallSegment、MiddleSegment、BigSegment 3中， 多个SmallSegment达到一定大小后合并到MiddleSegment，以此类推。按不同大小或时间拆分，也可以降低全量索引重建成本
5. 检索加速：支持非精准topk检索，postinglist归并时，支持按词频等静态分提前截断r个加速归并（胜者）。 归并后支持截断
6. 相关性打分：支持bm25相关性排序
7. 支持搜索词语义改写

## Requirement
- go 1.16.5 以上


## Quick Start
### 资源下载

- 下载项目代码到你的工作目录：

`git clone https://github.com/awesomefly/easysearch.git`

- 通过go mod更新依赖:

`go mod tidy`

- 下载wiki文档到本地路径

### 本地索引
- 构建索引文件

- 本地检索


### 分布式

- 构建分片索引

##### 分布式检索

- standalone模式

- 集群模式

### 语义改写
- 下载训练集
- 预处理
  - wiki文档预处理，提取词集 wiki2txt.py
- 模型训练
  - 采用python gensim.word2vec训练数据，并保存模型与词向量集合  word2vec.py
- 模型应用
  - golang使用code.sajari.com/word2vec库 加载训练得到的词向量集合， 通过api获取搜索词的近义词


[参考链接](https://artem.krylysov.com/blog/2020/07/28/lets-build-a-full-text-search-engine/.)


