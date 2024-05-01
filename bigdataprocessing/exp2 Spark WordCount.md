[toc]
# 2.1 spark
版本
spark3.5.1
scala2.12.18
java1.8.401
1. 安装目录
`/usr/local/spark`
2. 进入scala环境
`cd /usr/local/spark/bin`
`./spark-shell`


# 2.2 wordCount
## 2.2.1 命令行
1. 统计Hadoop单词个数
![f9bff7ab6cf34c97f177e316f1525925.png](../_resources/f9bff7ab6cf34c97f177e316f1525925-1.png)
![c2c217ffd0f514452059de3527abc0e9.png](../_resources/c2c217ffd0f514452059de3527abc0e9-1.png)
2. 每个单词个数
（原文档flatmap应是flatMap）
![6ca7cc2f230168b8a7909cf365b7057f.png](../_resources/6ca7cc2f230168b8a7909cf365b7057f-1.png)

## 2.2.2 Scala程序
![d89b99eb408983168b4e603a0f0c9f31.png](../_resources/d89b99eb408983168b4e603a0f0c9f31-1.png)
![3bccc0a7eb96c0f2d982d09c4fc6313b.png](../_resources/3bccc0a7eb96c0f2d982d09c4fc6313b-1.png)
- Scala原生：
![ed5f23e4c43f96990af57a41c3f22eee.png](../_resources/ed5f23e4c43f96990af57a41c3f22eee-1.png)
# 2.3 Spark Streaming
需要开启新终端，输入
`nc -lk 9999`
然后输入需要统计的字符串
![1cbc336bbf362b6189f97129360b8440.png](../_resources/1cbc336bbf362b6189f97129360b8440-1.png)
关闭端口所在的终端后，再次开启端口输入字符串，就无法获得单词统计了