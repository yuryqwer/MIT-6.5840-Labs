## Lab页面
[6.5840 Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

## 相关概念
- ***master***：读取输入文件，生成 map 任务；给 worker 分配 map 任务或者 reduce 任务；监控任务是否全部完成
- ***worker***：从 master 获取任务，针对 map 任务调用 Map 函数，针对 reduce 任务调用 Reduce 函数；master 无法连接则结束运行；所有任务都完成但是 master 还可以连接的时候，请求 master 分配任务时 master 可以返回一个特殊信息告知 worker 结束运行
- ***任务***：
  - map 任务：输入是一个文件名和文件内容，输出是 n 个中间文件（保存在本地），n 是 reduce 任务的数量。做 map 任务的 worker 从 master 那边获取文件内容，对这个文件调用 Map 函数，生成相应的键值对，然后使用`hash(key) % n`把键值对写到不同的中间文件中，上面的方法可以保证相同的 key 都在相同的文件中，并且每个文件中 key 种类的数量比较均匀（是种类的数量比较均匀，不是 key 的数量均匀，某些 key 相比其他 key 出现的次数可能多很多）
  - reduce 任务：输入是一堆中间文件，输出是一个文件。做 reduce 任务的 worker 从其他 worker 那边获取这个 reduce 任务对应的那些 map 任务输出的中间文件，汇总后排序，然后针对每个 key 和相关的值运行一次 Reduce 函数，把每次运行 Reduce 函数的结果写到文件中
- ***Map 函数***：输入是一个文件名和文件内容，输出是一些键值对（不要跟 map 任务弄混）
- ***Reduce 函数***：输入是一个键和这个键对应的值的集合，输出是根据这些值得到的一个结果