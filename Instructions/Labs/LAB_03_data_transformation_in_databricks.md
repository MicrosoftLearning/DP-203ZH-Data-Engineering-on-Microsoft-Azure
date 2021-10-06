---
lab:
    title: 'Azure Databricks 中的数据探索和转换'
    module: '模块 3'
---

# 实验室 3 - Azure Databricks 中的数据探索和转换

本实验室将指导你如何使用各种 Apache Spark DataFrame 方法探索和转换 Azure Databricks 中的数据。你将了解如何执行标准 DataFrame 方法以探索和转换数据。你还将了解如何执行更高级的任务，例如删除重复数据、操作数据/时间值、重命名列以及聚合数据。

完成本实验室后，你将能够：

- 使用 Azure Databricks 中的 DataFrame 探索和筛选数据
- 缓存 DataFrame 以加快后续查询
- 删除重复数据
- 操作日期/时间值
- 删除和重命名 DataFrame 列
- 聚合存储在 DataFrame 中的数据

## 实验室设置和先决条件

开始本实验室之前，请确保已成功完成创建实验室环境的安装步骤。此外，你还需要一个 Azure Databricks 群集（你应该已经在实验室 1 中创建了该群集）。如果未完成实验室 1（或已删除群集），以下说明包含创建群集的步骤。

## 练习 1 - 使用 DataFrame

在本练习中，你将通过使用一些 Databricks 笔记本，了解使用 DataFrame 的基础概念和技术。

### 任务 1：克隆 Databricks 存档

1. 如果当前没有打开 Azure Databricks 工作区：在 Azure 门户 (<https://portal.azure.com>) 中，导航到已部署的 Azure Databricks 工作区并选择“**启动工作区**”。
1. 在左侧窗格中，选择“**计算**”。如果你拥有现有群集，请确保其正在运行（根据需要启动）。如果你没有现有群集，请创建使用最新运行时和 **Scala 2.12** 或更高版本的单节点群集。
1. 当群集正在运行时，请在左窗格中，依次选择“**工作区**” > “**用户**”和你的用户名（带房屋图标的条目）。
1. 在显示的窗格中，选择名称旁边的箭头，然后选择“**导入**”。

    ![用于导入存档的菜单选项](images/import-archive.png)

1. 在“**导入笔记本**”对话框中，选择 URL 并粘贴以下 URL：

    ```
    https://github.com/MicrosoftLearning/DP-203-Data-Engineer/raw/master/Allfiles/microsoft-learning-paths-databricks-notebooks/data-engineering/DBC/04-Working-With-Dataframes.dbc
    ```

1. 选择“**导入**”。
1. 选择显示的 **04-Working-With-Dataframes** 文件夹。

### 任务 2：运行“*描述 DataFrame*”笔记本

1. 打开“**1.Describe-a-dataframe**”笔记本。
1. 将群集附加到笔记本中，然后按照说明运行笔记本中包含的单元格。在笔记本中，你将：
  - 熟悉 DataFrame API
  - 了解如何使用 **SparkSession** 和 **DataFrame**（也称为 ***Dataset[Row]***）类。
  - 了解如何使用“**计数**”操作。

### 任务 3：运行“*使用 DataFrame*”笔记本

1. 在 Azure Databricks 工作区的“**04-Working-With-Dataframes**”文件夹中，打开“**2.Use-common-dataframe-methods**”笔记本。
1. 将群集附加到笔记本上，然后按照说明操作并在笔记本中运行单元格。在笔记本中，你将：

    - 熟悉 DataFrame API
    - 使用常见的 DataFrame 方法提高性能
    - 探索 Spark API 文档

### 任务 4：运行“*显示函数*”笔记本

1. 在 Azure Databricks 工作区的“**04-Working-With-Dataframes**”文件夹中，打开“**3.Display-function**”笔记本。
1. 将群集附加到笔记本上，然后按照说明操作并在笔记本中运行单元格。在笔记本中，你将：

    - 了解如何使用以下转换：
      - limit(..)
      - select(..)
      - drop(..)
      - distinct()`
      - dropDuplicates(..)
    - 了解如何使用以下操作：
      - show(..)
      - display(..)

### 任务 5：完成“*不同的文章*”练习笔记本

1. 在 Azure Databricks 工作区的“**04-Working-With-Dataframes**”文件夹中，打开“**4.练习：不同的文章**”笔记本。
1. 将群集附加到笔记本上，然后按照说明操作并在笔记本中运行单元格。在此笔记本中，你会读取 Parquet 文件，应用必要的转换，执行记录的总计数，然后验证所有数据是否都正确加载。另外，可以尝试定义与数据匹配的架构，并更新读取操作以使用该架构。

    > 备注：你将在“**Solutions**”子文件夹中找到相应的笔记本。这包含该练习已完成的单元格。如果遇到困难或想要查看解决方案，请参阅笔记本。

## 练习 2 - 使用 DataFrame 高级方法

此练习以在上一个实验室中学到的 Azure Databricks DataFrame 概念为基础，探索了数据工程师可以通过使用 DataFrame 读取、写入和转换数据的一些高级方法。

### 任务 1：克隆 Databricks 存档

1. 在 Databricks 工作区的左侧窗格中，选择“**工作区**”，然后导航到主文件夹（带有房屋图标的用户名）。
1. 选中名称旁边的箭头，然后选择“**导入**”。
1. 在“**导入笔记本**”对话框中，选择 URL 并粘贴以下 URL：

    ```
    https://github.com/MicrosoftLearning/DP-203-Data-Engineer/raw/master/Allfiles/microsoft-learning-paths-databricks-notebooks/data-engineering/DBC/07-Dataframe-Advanced-Methods.dbc
    ```

1. 选择“**导入**”。
1. 选择显示的 **07-Dataframe-Advanced-Methods** 文件夹。

### 任务 2：运行“*日期和时间操作*”笔记本

1. 在 Azure Databricks 工作区的“**07-Dataframe-Advanced-Methods**”文件夹中，打开“**1.DateTime-Manipulation**”笔记本。
1. 将群集附加到笔记本上，然后按照说明操作并在笔记本中运行单元格。你将浏览更多 **sql.functions** 操作以及日期和时间函数。

### 任务 3：运行“*使用聚合函数*”笔记本

1. 在 Azure Databricks 工作区的“**07-Dataframe-Advanced-Methods**”文件夹中，打开“**2.Use-Aggregate-Functions**”笔记本。

1. 将群集附加到笔记本上，然后按照说明操作并在笔记本中运行单元格。在笔记本中，你将了解各种聚合函数。

### 任务 4：完成“*删除重复数据*”练习笔记本

1. 在 Azure Databricks 工作区的“**07-Dataframe-Advanced-Methods**”文件夹中，打开“**3.Exercise-Deduplication-of-Data**”笔记本。

1. 将群集附加到笔记本上，然后按照说明操作并在笔记本中运行单元格。本练习的目标是将所学的有关 DataFrame 的部分知识（包括重命名列）运用于实践。笔记本中提供了相关说明，并提供了空单元格供你完成工作。笔记本底部的其他单元格可帮助验证你的工作是否准确。

## 重要说明：关闭群集

1. 完成实验室后，在左侧窗格中，选择“**计算**”并选择群集。然后选择“**终止**”以停止群集。
