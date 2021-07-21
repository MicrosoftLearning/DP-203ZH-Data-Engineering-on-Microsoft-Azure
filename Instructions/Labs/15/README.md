# 模块 15 - 使用事件中心和 Azure Databricks 创建流式处理解决方案

在此模块中，学生将了解如何在 Azure Databricks 中使用事件中心和 Spark 结构化流大规模引入和处理流式数据。学生将了解结构化流的主要功能和使用方式。学生将实现滑动窗口以聚合数据块并应用水印以删除过时数据。最后，学生将连接到事件中心以读取流和写入流。

在本模块中，学生将能够：

- 了解结构化流的主要功能和使用方式
- 对文件中的数据进行流式处理，并将其写出到分布式文件系统
- 使用滑动窗口来聚合数据块（而不是所有数据）
- 应用水印以删除过时数据
- 连接到事件中心以读取流和写入流

## 实验室详细信息

- [模块 15 - 使用事件中心和 Azure Databricks 创建流式处理解决方案](#module-15---create-a-stream-processing-solution-with-event-hubs-and-azure-databricks)
  - [实验室详细信息](#lab-details)
  - [概念](#concepts)
  - [事件中心和 Spark Structured Streaming](#event-hubs-and-spark-structured-streaming)
  - [流式处理概念](#streaming-concepts)
  - [实验室](#lab)
    - [动手实验室准备工作](#before-the-hands-on-lab)
      - [任务 1：创建并配置 Azure Databricks 工作区](#task-1-create-and-configure-the-azure-databricks-workspace)
    - [练习 1：完成“结构化流概念”笔记本](#exercise-1-complete-the-structured-streaming-concepts-notebook)
      - [任务 1：克隆 Databricks 存档](#task-1-clone-the-databricks-archive)
      - [任务 2：完成笔记本](#task-2-complete-the-notebook)
    - [练习 2：完成“使用时间窗口”笔记本](#exercise-2-complete-the-working-with-time-windows-notebook)
    - [练习 3：完成“Azure 事件中心的结构化流”笔记本](#exercise-3-complete-the-structured-streaming-with-azure-eventhubs-notebook)

## 概念

Apache Spark Structured Streaming 是一种快速、可缩放且容错的流处理 API。可用它近实时地对流数据执行分析。

借助 Structured Streaming，可以使用 SQL 查询来处理流数据，其方式与处理静态数据的方式相同。API 不断递增并更新最终数据。

## 事件中心和 Spark Structured Streaming

Azure 事件中心是一种可缩放的实时数据引入服务，可在几秒钟内处理数百万个数据。Azure 事件中心可从多个源接收大量数据，并将准备好的数据流式传输到 Azure Data Lake 或 Azure Blob 存储。

Azure 事件中心可以与 Spark Structured Streaming 集成，执行近实时消息处理。使用 Structured Streaming 查询和 Spark SQL，可以在数据出现时查询和分析已处理的数据。

## 流式处理概念

流式处理是不断将新数据合并到 Data Lake 存储并计算结果的地方。流数据的传入速度比使用传统批处理相关技术对其进行处理时的速度更快。数据流被视为连续追加??数据的表。此类数据的示例包括银行卡交易、物联网 (IoT) 设备数据和视频游戏播放事件。

流式处理系统包括：

- Kafka、Azure 事件中心、IoT 中心、分布式系统上的文件或 TCP-IP 套接字等输入源
- 针对 forEach 接收器、内存接收器等使用结构化流进行流处理

## 实验室

你需要在 Databricks 笔记本中完成练习。首先，你需要有权访问 Azure Databricks 工作区。如果没有可用的工作区，请按照以下说明进行操作。否则，可跳到 `Clone the Databricks archive` 步骤。

### 动手实验室准备工作

> **备注：** 如果**不**使用托管实验室环境，而使用你自己的 Azure 订阅，则仅完成`Before the hands-on lab`步骤。否则，请跳转到练习 1。

在分步完成此实验室中的练习时，请确保可访问具有可用群集的 Azure Databricks 工作区。执行以下步骤以配置工作区。

#### 任务 1：创建并配置 Azure Databricks 工作区

按照[实验室 15 设置说明](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/blob/main/setup/15/lab-01-setup.md)创建并配置工作区。

### 练习 1：完成“结构化流概念”笔记本

#### 任务 1：克隆 Databricks 存档

1. 如果当前没有打开 Azure Databricks 工作区：在 Azure 门户中，导航到已部署的 Azure Databricks 工作区并选择 **“启动工作区”**。
1. 在左窗格中，依次选择 **“工作区”** > **“用户”**，然后选择用户名（带房屋图标的条目）。
1. 在显示的窗格中，选择名称旁边的箭头，然后选择 **“导入”**。

    ![用于导入存档的菜单选项](media/import-archive.png)

1. 在 **“导入笔记本”** 对话框中，选择 URL 并粘贴以下 URL：

 ```
  https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/10-Structured-Streaming.dbc?raw=true
 ```

1. 选择 **“导入”**。
1. 选择出现的 **“10-Structured-Streaming”** 文件夹。

#### 任务 2：完成笔记本

打开 **“1.Structured-Streaming-Concepts”** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 对文件中的数据进行流式处理，并将其写出到分布式文件系统
- 列出活动的流
- 停止活动的流

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 2：完成“使用时间窗口”笔记本

在 Azure Databricks 工作区中，打开你在用户文件夹中导入的 **“10-Structured-Streaming”** 文件夹。

打开 **“2.Time-Windows”** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 使用滑动窗口来聚合数据块（而不是所有数据）
- 应用水印以丢弃没有空间保留的过时旧数据
- 使用`display`绘制实时图

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 3：完成“Azure 事件中心的结构化流”笔记本

在 Azure Databricks 工作区中，打开你在用户文件夹中导入的 **“10-Structured-Streaming”** 文件夹。

打开 **“3.Streaming-With-Event-Hubs-Demo”** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 连接到事件中心并将流写入事件中心
- 从事件中心读取流
- 定义 JSON 有效负载的架构并分析数据，以在表中显示数据
