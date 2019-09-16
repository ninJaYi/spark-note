## Spark 核心架构



![a](https://github.com/ninJaYi/spark-note/blob/master/etc/spark%E6%A0%B8%E5%BF%83%E6%9E%B6%E6%9E%84.jpg)

### 流程分析

* spark-submit 提交spark程序到Application,通过反射构造DriverActory进程

* Spark context: 初始化 构造DAGSchedule和TaskScheduler

	* 每执行到一个action 就会创建一个job提交给DAGSchedule，DAGSchedule会将job切分

		为多个stage,然后为每一个stage创建一个TaskSet

* TaskScheduler: 对应的后台进程连接spark集群Master节点,向Master注册Application

* Master: 接收到Application注册的请求后，会使用自己的资源调度算法，spark集群的Worker上，为这个Applicaton启动多个Executer

* Worker:为Application启动Executer

* Executer(进程)：Executer启动之后会将自己反向注册到TaskScheduler上去

	* TaskScheduler会把TaskSet里每个task 提交到Executer上去执行（Task 分配算法）

* TaskRunner: Executer每接收到一个task都会用TaskRunner来封装task,然后从线程池中分配一个线程，执行task

	* 将要执行算子以及函数，拷贝，反序列化，执行task

