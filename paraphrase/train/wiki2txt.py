# -*- coding: utf-8 -*-
import logging
import sys
from gensim.corpora import WikiCorpus
import opencc

dir_path = "data/"


def read_sample():
    i = 0
    with open(dir_path + "wiki_texts.txt", 'r') as f:
        for line in f:
            print(line)
            if i == 10:
                return
            i += 1


# train corpus source  https://dumps.wikimedia.org/enwiki/latest/
# xml to txt
def wiki_to_txt(file):
    if file is None:
        return

    corpus_path = file #"~/Downloads/enwiki-latest-pages-articles11.xml-p6899367p7054859.bz2"
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    output = open(dir_path + "wiki_texts.txt", 'w')
    wiki = WikiCorpus(corpus_path, processes=15, dictionary={})
    i = 0
    for text in wiki.get_texts():
        output.write(" ".join(text) + "\n")
        i += 1
        if i % 10000 == 0:
            logging.info("Saved " + str(i) + " articles")
    output.close()
    logging.info("Finished Saved " + str(i) + " articles")


def convert2simple():
    cc = opencc.OpenCC('t2s')
    for i in range(1, 5):
        src_file = dir_path + "wiki_texts" + str(i) + ".txt"
        des_file = dir_path + "wiki_simple" + str(i) + ".txt"
        des_f = open(des_file, 'w')
        with open(src_file, 'r') as f:
            for line in f:
                # print line.decode('utf-8')
                content = cc.convert(line.decode('utf-8'))
                print(content)
                des_f.write(content.encode('utf-8') + '\n')
        des_f.close()
        print(str(i) + " finished.")


# /usr/bin/python3 paraphrase/train/wiki2txt.py --cmd=parse --file=$WIKI_FILE
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmd", type=str, required=True)
    parser.add_argument("--file", type=str, required=False)
    args = parser.parse_args()
    if args.cmd == 'parse':
        wiki_to_txt(args.file)
    elif args.cmd == 'sample':
        read_sample()
    # convert2simple()
