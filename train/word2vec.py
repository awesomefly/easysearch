# -*- coding: utf-8 -*-
import string

from gensim.models import word2vec, KeyedVectors
import logging

corpus_dir = "/Users/bytedance/go/src/github.com/simplefts/data/"
project_dir = "/Users/bytedance/go/src/github.com/simplefts/data/"


# 训练word2vec模型
def train():
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    corpus_path = corpus_dir + 'wiki_texts.txt'
    model_path = project_dir + 'med200_less.model.bin'
    model_word2vec_format_path = project_dir + 'model.word2vec.format.bin'

    sentences = word2vec.LineSentence(corpus_path)
    model = word2vec.Word2Vec(sentences, vector_size=200)

    # 保存模型，供日後使用
    model.save(model_path)

    # 按word2vec格式存储向量信息
    model.wv.save_word2vec_format(model_word2vec_format_path, binary=True)


def similar_test(positive=None, negative=None):
    model_path = project_dir + 'med200_less.model.bin'
    model_word2vec_format_path = project_dir + 'model.word2vec.format.bin'

    # model = word2vec.Word2Vec.load(model_path)
    model = KeyedVectors.load_word2vec_format(model_word2vec_format_path, binary=True)

    try:
        # items = model.wv.most_similar(positive, negative, topn=10)
        items = model.most_similar(positive, negative, topn=10)
        for item in items:
            print(item[0].encode('utf-8'), item[1])
    except Exception as e:
        print(repr(e))


if __name__ == "__main__":
    # train()

    positive = ['king', 'woman']
    negative = ['man']
    similar_test(positive, negative)
