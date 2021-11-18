# Simple Full-Text Search engine

Sample code for https://artem.krylysov.com/blog/2020/07/28/lets-build-a-full-text-search-engine/.


新特性：

- 支持非精准topk检索，posting list采用胜者表，按词频提前截断r个。 归并后按bm25得分排序
- 搜索词语义改写


语义改写
- wiki文档预处理，提取词集 wiki2txt.py
- 采用python gensim.word2vec训练数据，并保存模型与词向量集合  word2vec.py
- golang使用code.sajari.com/word2vec库 加载训练得到的词向量集合， 通过api获取搜索词的近义词
