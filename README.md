### 实验一 Map Recude
Worker 通过 GetTask 与 NotifyTask 与 master 进行沟通。如果 woker 完成一个任务，向 master 调用 NotifyTask 将任务提交，master 修改相应的内存中的数据结构。
在 master 中维护一个状态机根据状态来给 Worker 排法 reduce or map 的任务。流程图如下图

![](https://tva1.sinaimg.cn/large/00831rSTgy1gd08hg0jvbj30u017vdpw.jpg)