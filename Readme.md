## 总体思路

​        根据词频、左右熵、互信息提取候选; 然后使用简单的分类器过滤明显不是电力专业的词汇。



### 1：提取候选

a) 准备背景语料(这里用的sougou的语料)记做语料A，将语料A和电力语料合并在一起记做语料B。语料A和语料B都执行b~e步

b) 将语料按行去重，使用hanlp分词

c) 对分词后的行分句，如下条件都是句子的切分点

​    i)词性为标点符号、连词，助词，介词，代词

​    ii)词包含非中文字

​    iii)词是停用词

d) 过滤词频小于5的词；统计词左右熵(左熵和右熵的最小值)，如果词为在句首，则默认词的左熵有充分的多样性(每出现一次都认为左边的词不一样)，如果词在句尾，则默认词的右熵有充分的多样性;过滤左右熵小于1.5的词

e) 统计词的互信息,一个词可能有多重此组合模型，取互信息最大的那个;过滤互信息小于1的词。单个词的互信息不能计算,按照如下公式给值，词的长度越长互信息值越大

```scala
0.8 * word.length + 0.5 //长度的线性值
```

f) 比较语料B和语料A的统计结果;获取电力专业词候选，如下两个规则提取候选

   i)语料B中有的词，语料A中没有的词

   ii)语料B和语料A中都有的词，语料B中的词频超过语料A中的10倍,并且左右熵或者互信息也大一些。



### 2: 过滤和排序

#### 2.1 过滤

​        使用svm训练二分类器，过滤掉预测值小于阈值(0.35)的候选

**正样本**

​         来自百科文库中搜索电力专业词汇一共6000k多个在/src/main/resources/positive_dict.txt中

**负样本**

​         a)来自背景语料中随机采样的，保存在/src/main/resources/negtive_sample.txt中

​         b)人工整理的论文期刊词汇，保存在src/main/resources/negtive_paper_word.txt中

​         c) 机构类负样本，保存在src/main/resources/negtive_nz.txt中

**特征**

​       一共选取了13个特征，具体如下：

​       a)字类: 组成候选词的字符的逆文档字频(最大值、最小值、和、平均值)，所有逆文档字频的信息保存在out/charidf/part-00000中,来自背景语料的统计;

​       b)词类：候选词(一般是短语)分词后的逆文档词频(最大值、最小值、和、平均值)，所有逆文档词频的信息保存在out/idf/part-00000中,来自背景语料的统计;

​       c) 热点字类：组成候选词的字在正样本总出现的频次(平均值，和)

​       d) 热点词类：候选词(一般是短语)分词后词在正样本中出现的频次(平均值，和)

​       e) 其它：候选词的长度

​     

#### 2.2 排序

​        按照 `词频*左右熵*互信息*长度因子*svm分类预测值` 倒序

长度因子定义代码如下：

```scala
def lengthFactor(word: String): Double = {
  1.0 / Math.exp(Math.abs(word.length - 4.5) * 0.3)
}
```

​        太长或者太短的词都不太可能是专业名称，长度4和5有最大可能性

## 运行工程

### 数据准备

1: 将下载搜狗背景语料放在工程的out/news目录下

2: 将下载搜狗背景语料和比赛的电力词汇放在out/news_elec目录下



### 中间结果统计

1：分词

入口类：com.es.analyze.Segment

参考：local[8] out/news out/segment

2: 逆文档词频统计

入口类：com.es.analyze.WordFrequecy ；

参数：local[8] out/segment out/idf

在main方法中执行如下：

```scala
saveidf(spark, inputPath, outPath)
```

3: 逆文档字频统计

入口类：com.es.analyze.WordFrequecy ；

参数：local[8] out/segment out/charidf

在main方法中执行如下：

```scala
saveCharidf(spark, inputPath, outPath)
```



### 候选提取

入口类：com.es.analyze.Main；

参数：无

jvm参数：-Xmx8g -Xms8g

在main方法中执行如下：

```scala
        //背景语料左右熵
        Entropy.entropyMain("local[16] out/news out/entropy_bg 2048".split(" "))
        //背景和前景语料左右熵
        Entropy.entropyMain("local[16] out/news_elec out/entropy_bg_elec 2048".split(" "))

        //背景和前景语料词频统计
        Entropy.freqMain("local[16] out/news_elec out/freq_bg_elec 256".split(" "))

        //互信息统计(背景)
        Pmi.PmiMain("local[4] out/entropy_bg/part-00000 out/entropy_pmi_bg.txt out/freq_bg_elec/part-00000".split(" "))
        //互信息统计(背景和前景)
        Pmi.PmiMain("local[4] out/entropy_bg_elec/part-00000 out/entropy_pmi_bg_elec.txt out/freq_bg_elec/part-00000".split(" "))

    //通过比较获取候选结果
    dealDiff("out/entropy_pmi_bg.txt", "out/entropy_pmi_bg_elec.txt")
```



### 生成候选特征

入口类：com.es.analyze.FeatureMain；

参数：无

输出：训练文件-out/train_filter.txt,预测文件-out/predict_filter.txt



### 训练和预测

​         此部分在python工程中，执行svm.py文件即可

输出：out\pred_filter.txt 带有预测值



### 过滤和排序得到最终结果

入口类：com.es.analyze.Main；

参数：无

在main方法中执行如下：

```scala
resultMain()
evalutor()
```