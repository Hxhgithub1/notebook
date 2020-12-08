# spark

## 历史简介

hadoop1.0版本

![](C:\Users\yzy\Desktop\日志\image-20201021101621597.png)

文件系统核心模块(HDFS)：
**NameNode**：集群当中的主节点，主要用于管理集群当中的各种数据
**secondaryNameNode**：主要能用于hadoop当中元数据信息的辅助管理
**DataNode**：集群当中的从节点，主要用于存储集群当中的各种数据
数据计算核心模块(MapReduce)：
**JobTracker**：接收用户的计算请求任务，并分配任务给从节点
**TaskTracker**：负责执行主节点JobTracker分配的任务

2.0版本

![image-20201021102708372](C:\Users\yzy\Desktop\日志\image-20201021102708372.png)

文件系统核心模块(HDFS)：
**NameNode**：集群当中的主节点，主要用于管理集群当中的各种数据
**secondaryNameNode**：主要能用于hadoop当中元数据信息的辅助管理
**DataNode**：集群当中的从节点，主要用于存储集群当中的各种数据
资源调度（yarn）：
**ResourceManager**：接收用户的计算请求任务，并负责集群的资源分配，以及计算任务的划分

在rm中加了一层	AM(Application Master) ,通过am调用Driver

**NodeManager**：负责执行主节点ResourceManager分配的任务

提出yarn,降低其耦合度

其中包含Container,将资源和计算分开降低耦合性，Container里面放task

**spark**

![image-20201021105448735](C:\Users\yzy\Desktop\日志\image-20201021105448735.png)

2.2.1Master
Spark 特有资源调度系统的 Leader。掌管着整个集群的资源信息，类似于 Yarn 框架中的 ResourceManager，主要功能：

1. 监听 Worker，看 Worker 是否正常工作；

Master 对 Worker、Application 等的管理(接收 Worker 的注册并管理所有的Worker，**接收 Client 提交的 Application，调度等待的 Application 并向Worker 提交**)。
2.2.2Worker
Spark 特有资源调度系统的 Slave，有多个。每个 Slave 掌管着所在节点的资源信息，类似于 Yarn 框架中的 NodeManager，主要功能：

1. 通过 RegisterWorker 注册到 Master；
2. 定时发送心跳给 Master；
3. **根据 Master 发送的 Application 配置进程环境，并启动 ExecutorBackend**(执行 Task 所需的临时进程)

### hadoop生态各部分的作用

Hadoop 中各模块的作用： 

1、Hadoop HDFS为HBase提供了高可靠性的底层存储支持。

2、Hadoop MapReduce为HBase提供了高性能的计算能力。

3、Zookeeper为HBase提供了稳定服务和failover机制。

4、Pig和Hive还为HBase提供了高层语言支持，使得在HBase上进行数据统计处理变得非常简单。

5、Sqoop则为HBase提供了方便的RDBMS（关系型数据库）数据导入功能，使得传统数据库数据向HBase中迁移变得非常方便。

spark的数据存储是根据分区数决定的，文件的字节数决定的

## Spark的工作流程

### 基本概念的理解

在开始之前需要先了解Spark中Application，Job，Stage等基本概念，官方给出的解释如下表：

| Term            | Meaning                                                     |
| :-------------- | :---------------------------------------------------------- |
| Application     | 用户编写的Spark应用程序,包括一个Driver和多个executors       |
| Application jar | 包含用户程序的Jar包                                         |
| Driver Program  | 运行main()函数并创建SparkContext**进程**                    |
| Cluster manager | 在集群上获取资源的外部服务，如standalone manager,yarn,Mesos |
| deploy mode     | 部署模式，区别在于driver process运行的位置                  |
| worker node     | 集群中可以运行程序代码的节点（机器）                        |
| Executor        | 运行在worker node上执行具体的计算任务，存储数据的**进程**   |
| Task            | 被分配到一个Executor上的计算单元                            |
| Job             | 由多个任务组成的并行计算阶段，因RDD的Action产生             |
| Stage           | 每个Job被分为小的计算任务组，每组称为一个stage              |
| DAGScheduler    | 根据Job构建基于Stage的DAG，并提交Stage给TaskScheduler       |
| TaskScheduler   | 将TaskSet提交给worker运行，每个executor运行什么task在此分配 |

作者：由木人_番茄
链接：https://www.jianshu.com/p/3aa52ee3a802

https://www.cnblogs.com/ITtangtang/p/7967386.html

**job**

 一个spark的程序可以被划分为多个job，划分的依据是每遇到一个Action操作就生成一个新的job，

**Stage**

每个job又可以划分为一个或多个可以并行计算的Stage,划分的依据是是否进行了shuffle操作（即根据rdd之间的dependency关系），宽依赖时会产生shuffle操作，会划分为不同的stage

**Task**

***stage是由task组成的并行计算***(根据分区交由不同的executer计算)，一般一个rdd的分区对应一个task,task又分为ResultTask和ShuffleMapTask。经过shuffle后rdd的分区改变，task数也会改变。

**partition**

通常一个RDD被划分为一个或多个Partition，Partition是Spark进行数据处理的基本单位，**一般来说一个Partition对应一个Task，而一个Partition中通常包含数据集中的多条记录(Record)**。
 注意不同Partition中包含的记录数可能不同。Partition的数目可以在创建RDD时指定，也可以通过reparation和coalesce等算子重新进行划分。

**master和worker**

一个集群可能有多个master节点和多个worker节点，

master节点常驻master守护进程，负责管理worker节点，我们从master节点提交应用。

worker节点常驻worker守护进程，与master节点通信，并且管理executor进程。

一台机器可以同时作为master和worker节点

**driver和executor进程**

1.driver进程就是应用的main()函数并且构建sparkContext对象，当我们提交了应用之后，便会启动一个对应的driver进程，driver本身会根据我们设置的参数占有一定的资源（主要指cpu core和memory）

2.driver可以运行在master上，也可以运行worker上（根据部署模式的不同）。driver首先会向集群管理者（standalone、yarn，mesos）申请spark应用所需的资源，也就是executor，然后集群管理者会根据spark应用所设置的参数在各个worker上分配一定数量的executor，每个executor都占用一定数量的cpu和memory

3.在申请到应用所需的资源以后，driver就开始调度和执行我们编写的应用代码了。driver进程会将我们编写的spark应用代码拆分成多个stage，每个stage执行一部分代码片段，并为每个stage创建一批tasks，然后将这些tasks分配到各个executor中执行。

4.executor进程宿主在worker节点上，一个worker可以有多个executor。每个executor持有一个线程池，每个线程可以执行一个task，executor执行完task以后将结果返回给driver，每个executor执行的task都属于同一个应用

也就是说我们在提交代码到集群上并启动执行之后会执行以下操作：

启动driver ===> driver向集群管理者申请executer ===> Cluster manager会根据参数在各个worker上分配一定数量的executer ===>driver将代码拆分为stage,每个stage又由多个task组成 ====>将task分配给executer执行 ===> 执行完结果返回给driver

worker与executer是一对多

executer与task是一对一的关系

 **宽依赖**

具有宽依赖的 transformations 包括: sort, reduceByKey, groupByKey, join, 和调用rePartition函数的任何操作

### actions操作

获取元素
collect(), first(), take(*n*), takeSample(withReplacement, num, [seed]),  takeOrdered(n, [ordering])

计数元素
count(), countByKey()

迭代元素
reduce(*func*),  foreach(*func*)

保存元素
saveAsTextFile(*path*), saveAsSequenceFile(*path*), saveAsObjectFile(*path*)
链接：https://www.imooc.com/article/267796

```
观察函数的返回类型：如果返回的是  RDD 类型，那么这是 transformation; 如果返回的是其他数据类型，那么这是 action.
```



### 算子的分类

1、Trasformtion算子
union、reduceByKey、groupBy、join、map、mapPartition
、cogroup、parallelize、textFile、leftoutJoin、flatMap、coalesce、Repartition

2、Action算子（action时触发job）
count、take、collect、foreach、foreachPartition、saveAsTextFile、ditinct、
first、reduce

3、shuffle算子（shuffer时产生stage）
reduceByKey、groupBy、join、coalesce、Repartition、leftoutJoin、cogroup

4、产生Stage划分的算子
reduceByKey、groupBy、join、coalesce、Repartition、leftoutJoin、cogroup

5、触发Job算子

count、take、collect、foreach、foreachPartition、saveAsTextFile、ditinct、
first、reduce

[各算子的介绍](https://blog.csdn.net/weixin_38750084/article/details/83145432#%EF%BC%8820%EF%BC%89join)

https://blog.csdn.net/do_what_you_can_do/article/details/53155555

```json
"x":116.321551,"y":40.008945
"x":116.323722,"y":40.008761
```

使用zippartions的话每个分区第一个会自动分隔，那如何设置分区呢？

根据自定义的时间切分？怎么切？

要解决的问题：

1. join的优化（完成）

2. 输出多边形，输出中心点

3. 累加器与预停留点有序无序问题（完成）

4. 预留参数

5. 读入整个数据集（完成）

   （1）用正则表达式筛选文件

   （2）每个文件存到一个数组里

   （3）自定义分区，根据用户和天数的不同

6. 怎样按分区输出呢？

不足之处在哪？



### 矢量数据的存储

1. 如何让矢量数据均匀的分布

2. 矢量数据的格式和存储方式，可以把矢量数据根据范围划分成网格，储存每个网格的图形数，点数，达到负载均衡，计算每个节点分布多少个格子。

   

## RDD编程

### RDD的创建

1. 从集合中创建（两种方式）

```scala
 val config = new SparkConf().setMaster("local[*]").setAppName("RDD01")
    val sc = new SparkContext(config)
   //rdd默认的分区是由内核数决定的
    val listRDD = sc.makeRDD(List(1, 2, 3, 4))
    val arrayRDD = sc.parallelize(Array(1, 2, 3, 4))
    listRDD.collect().foreach(println)
    
    arrayRDD.collect().foreach(println)
```

2. 从外部存储中创建

```scala
	//2.从外部存储中创建
    val value = sc.textFile("in",2)
    value.collect().foreach(println)
    value.saveAsTextFile("output")
```

3. 从其他 RDD 转换得到新的 RDD

就是通过 RDD 的各种转换算子来得到新的 RDD.

### RDD的转换

#### value类型

##### 2.3.1.1   map(func)

作用: 返回一个新的 RDD, 该 RDD 是由原 RDD 的每个元素经过函数转换后的值而组成. 就是对 RDD 中的数据做转换.

创建一个包含1-10的的 RDD，然后将每个元素*2形成新的 RDD

```scala
	val value = sc .makeRDD(1 to 10)
    val value1 = value.map(_* 2)
    value1.collect().foreach(println)
```

##### mapPartitions

遍历RDD中所有的分区

效率优于map，减少了发送到执行器执行的交互次数

可能会出现内存溢出（oom）

```scala
 	val value = sc .makeRDD(1 to 10)
    val value1 = value.mapPartitions(datas=>{
      datas.map(_*2)
    })
    value1.collect().foreach(println)
```

##### mapPartitionsWithIndex

遍历分区，且拿到分区号

```scala
 val value = sc .makeRDD(1 to 10,2)
    val value1 = value.mapPartitionsWithIndex((num,datas)=>{
      datas.map((_,"分区号"+num))
    })
    value1.collect().foreach(println)
  }
输出结果为：
(1,分区号0)
(2,分区号0)
(3,分区号0)
(4,分区号0)
(5,分区号0)
(6,分区号1)
(7,分区号1)
(8,分区号1)
(9,分区号1)
(10,分区号1)
```

##### flatMap(func)

将func应用到rdd的所有元素后，并对rdd进行扁平化处理

```scala
 val value = sc .makeRDD(Array(List(1,2),List(3,4)))
    val value1 = value.flatMap(datas=>datas)
    value1.collect().foreach(println)
```

##### glom()

将每个分区的元素合并成一个数组，如果想对各个分区中的数据进行操作，用它比较方便

```scala
//将各个分区的内容打印出来   
val value = sc .makeRDD(1 to 16 ,4 )
    val value1 = value.glom()
    value1.collect().foreach(array=>{println(array.mkString(","))})
```

##### groupBy

按照func的返回值对rdd进行分组

```scala
    //分组后形成了对偶元组，k表示分组的key，v表示分组的数据集合
    val value = sc .makeRDD(1 to 16 )
    val value1 = value.groupBy(i=>i%2)
    value1.collect().foreach(println)
```

##### 2.3.1.1   filter(func)

作用: 过滤. 返回一个新的 RDD 是由func的返回值为true的那些元素组成

```scala
	val value = sc .makeRDD(1 to 16 )
    val value1 = value.filter(i=>i%2==0)
    value1.collect().foreach(println)
```

##### sample

抽样sample(withReplacement, fraction, seed)

withReplacement:是否可放回

fraction：抽样比例

seed：种子数

```scala
	val value = sc .makeRDD(1 to 16 )
    val value1 = value.sample(false,0.5 ,1)
    value1.collect().foreach(println)
```

#####  coalesce(numPartitions，shuffle)

作用: 缩减分区数到指定的数量，用于大数据集过滤后，提高小数据集的执行效率。

```scala
	val value = sc .makeRDD( 1 to 16 ,4)
    val value1 = value.coalesce(3)
    value.saveAsTextFile("output")
```

##### repartition(numPartitions)

作用: 根据新的分区数, 重新 shuffle 所有的数据, 这个操作总会通过网络.

新的分区数相比以前可以多, 也可以少

repartition其实就是对coalesce进行了封装，将shuffle设置为了true

#####  sortBy(func,[ascending], [numTasks])

作用: 使用func先对数据进行处理，按照处理后结果排序，默认为正序。numTasks:分区数

```scala
    val value = sc .makeRDD( 1 to 16 )
    val value1 = value.sortBy((num=>num),false,2)
    value1.saveAsTextFile("output")
```

#### 双value类型

#### k-v类型



### spark中对于文件类型分区数，和存储的文件数的确定的确定

如 在sc.textFile中设置分区数，假设一个文件13个字节。

1. 若为设置的分区数为2则用13%2=6,6,1。所以会产生三个文件用于存储数据（但是并不是每个文件中都是有内容的）。**当字节总数除以分区数大于9时则最终产生的存储数据的文件数=分区数，不再考虑余数**（这里不太理解，不知道为啥）。

2. 每个文件中存入的内容要大于相应的字节数（阈值，第一个文件要大于6，所以至少为7）。
3. spark会按行读取，无论该行数据量为多少都会一次性读完写到第一个分区文件中，并检查是否大于阈值（这里为6），大于阈值则继续读取下一行并写到下一个文件中，如果小于或等于阈值则接着读取下一行，直至大于阈值（spark读取文件是以行为单位，不会把一行单独拆开放到一个文件中）

### RDD的持久化

我们已经知道job是根据action来划分的，而每调用一次action类型的算子都会创建一个新的 job, 每个 job 总是从它血缘的起始开始计算. 所以, 会发现中间的这些计算过程都会重复的执行.

```scala
object CacheDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("ab", "bc"))
        val rdd2 = rdd1.flatMap(x => {
            println("flatMap...")
            x.split("")
        })
        val rdd3: RDD[(String, Int)] = rdd2.map(x => {
            (x, 1)
        })
        rdd3.collect.foreach(println)
        println("-----------")
        rdd3.collect.foreach(println)
    }
```

![image-20201106085400010](C:\Users\yzy\Desktop\日志\image-20201106085400010.png)

会发现在调用第二个collect的时候又把该job的所有流程都计算了一遍，而完全没有这个必要。

所以引入数据持久化，将所需的rdd结果保存起来（可以是内存，硬盘。。。）使得计算效率更快。

可以使用方法persist()或者cache()来持久化一个 RDD. 在第一个 action 会计算这个 RDD, 然后把结果的存储到他的节点的内存中.

```scala
// rdd2.cache() // 等价于 rdd2.persist(StorageLevel.MEMORY_ONLY)
rdd2.persist(StorageLevel.MEMORY_ONLY)
```

![image-20201106085829553](C:\Users\yzy\Desktop\日志\image-20201106085829553.png)

说明:

\1.     第一个 job 会计算 RDD2, 以后的 job 就不用再计算了.

\2.     有一点需要说明的是, 即使我们不手动设置持久化, Spark 也会自动的对一些 shuffle 操作的中间数据做持久化操作(比如: reduceByKey). 这样做的目的是为了当一个节点 shuffle 失败了避免重新计算整个输入. 当时, 在实际使用的时候, 如果想重用数据, 仍然建议调用persist 或 cache\

### 设置检查点

Lineage 过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果之后有节点出现问题而丢失分区，从做检查点的 RDD 开始重做 Lineage，就会减少开销

为当前 RDD 设置检查点。该函数将会创建一个二进制的文件，并存储到 checkpoint 目录中，该目录是用 SparkContext.setCheckpointDir()设置的。在 checkpoint 的过程中，该RDD 的所有依赖于父 RDD中 的信息将全部被移除。

```scala
package day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object CheckPointDemo {
    def main(args: Array[String]): Unit = {
        // 要在SparkContext初始化之前设置, 都在无效
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        // 设置 checkpoint的目录. 如果spark运行在集群上, 则必须是 hdfs 目录
        sc.setCheckpointDir("hdfs://hadoop201:9000/checkpoint")
        val rdd1 = sc.parallelize(Array("abc"))
        val rdd2: RDD[String] = rdd1.map(_ + " : " + System.currentTimeMillis())

        /*
        标记 RDD2的 checkpoint.
        RDD2会被保存到文件中(文件位于前面设置的目录中), 并且会切断到父RDD的引用, 也就是切断了它向上的血缘关系
        该函数必须在job被执行之前调用.
        强烈建议把这个RDD序列化到内存中, 否则, 把他保存到文件的时候需要重新计算.
         */
        rdd2.checkpoint()
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)
    }
}
```

![image-20201106092312756](C:\Users\yzy\Desktop\日志\image-20201106092312756.png)

![image-20201106092320155](C:\Users\yzy\Desktop\日志\image-20201106092320155.png)

### 持久化和checkpoint的区别

1. 持久化不改变rdd的依赖，而checkpoint改变了rdd的依赖
2. 持久化数据通常保存在内存或硬盘中，而checkpoint数据常保存在hdfs中容错性高
3. 没有持久化，就设置检查点，要将 RDD 的数据写入外部文件系统的话，需要全部重新计算一次，所以建议在设置检查点前先持久化

## RDD编程进阶

spark有三大数据结构：rdd,广播变量，累加器

广播变量：分布式只读共享变量

累加器：分布式只写共享变量

## 集群中运行spark

## standalong模式

首先把在idea中写好的程序通过maven打成jar包，然后到集群环境的master节点下输入命令

```shell
bin/spark-submit 
--class com.hxh.spark.phoneSort 
--master spark://192.168.202.103:7077 
spark01-1.0-SNAPSHOT.jar  phoneFlow out/outputphone
```

class是phoneSort的主类名

master是集群的启动方式local[*]为本地，yarn为yarn启动，standalong模式的话为spark://master节点的ip:7077

然后是jar包的名称，最后两个参数为输入输出路径，在程序中指定
