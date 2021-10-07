---
lab:
    title: '使用 Azure Synapse Link 支持混合事务分析处理 (HTAP)'
    module: '模块 9'
---

# 实验室 9 - 使用 Azure Synapse Link 支持混合事务分析处理 (HTAP)

在此实验室中，你将了解如何使用 Azure Synapse Link 将 Azure Cosmos DB 帐户无缝连接到 Synapse 工作区。你将了解如何启用并配置 Synapse Link，以及如何使用 Apache Spark 和 SQL 无服务器查询 Azure Cosmos DB 分析存储。

完成本实验室后，你将能够：

- 使用 Azure Cosmos DB 配置 Azure Synapse Link
- 使用 Apache Spark for Synapse Analytics 查询 Azure Cosmos DB
- 使用 Azure Synapse Analytics 中的无服务器 SQL 池查询 Azure Cosmos DB

## 实验室设置和先决条件

开始该实验室之前，应先完成“实验室 6：使用 Azure 数据工厂或 Azure Synapse 管道转换数据”。

> **备注**：如果你尚***未***完成实验室 6，但已<u>完成</u>本课程的实验室设置，则可以完成这些步骤以创建所需的链接服务和数据集。
>
> 1. 在 Synapse Studio 的“**管理**”中心，使用以下设置为 **Azure Cosmos DB (SQL API)** 添加新的**链接服务**：
>       - **名称**：asacosmosdb01
>       - **Cosmos DB 帐户名**：asacosmosdb*xxxxxxx*
>       - **数据库名称**：CustomerProfile
> 2. 在“**数据**”中心，创建以下“**集成数据集**”：
>       - **源**：Azure Data Lake Storage Gen2
>       - **格式**：JSON
>       - **名称**：asal400_customerprofile_cosmosdb
>       - **链接服务**：asadatalake*xxxxxxx*
>       - **文件路径**：wwi-02/online-user-profiles-02
>       - **导入架构**：从连接/存储

## 练习 1 – 使用 Azure Cosmos DB 配置 Azure Synapse Link

Tailwind Traders 使用 Azure Cosmos DB 存储来自其电子商务网站中的用户资料数据。可借助 Azure Cosmos DB SQL API 提供的 NoSQL 文档来熟悉如何使用 SQL 语法管理这些数据，同时大规模地全局读取和写入文件。

虽然 Tailwind Traders 对 Azure Cosmos DB 的功能和性能很满意，但他们想知道在数据仓库中的多个分区上执行大量分析查询（跨分区查询）会产生多少费用。他们希望能在不增加 Azure Cosmos DB 请求单位 (RU) 的前提下高效访问所有数据。他们已考虑过一些方案，可在 Data Lake 发生变化时通过 Azure Cosmos DB 更改源机制将容器中的数据提取到数据库。此方法的问题是会产生额外的服务和代码依赖项以及需要对解决方案进行长期维护。可以从 Synapse Pipeline 执行批量导出，但其中并不包含任何给定时刻的最新信息。

你决定为 Cosmos DB 启用 Azure Synapse Link，并在 Azure Cosmos DB 容器上启用分析存储。使用此配置，所有事务数据会自动存储在完全隔离的列存储中。此存储可实现针对 Azure Cosmos DB 中操作数据的大规模分析，且不会影响事务工作负载，也不会产生资源单元 (RU) 成本。适用于 Cosmos DB 的 Azure Synapse Link 在 Azure Cosmos DB 与 Azure Synapse Analytics 之间建立了紧密的集成，使 Tailwind Traders 能对其操作数据运行准实时分析，同时无需 ETL 过程，且完全不影响事务工作负载的性能。

通过将 Cosmos DB 的事务处理的分布式缩放与 Azure Synapse Analytics 内置的分析存储和计算能力相结合，Azure Synapse Link 可实现混合事务/分析处理 (HTAP) 体系结构，从而优化 Tailwind Trader 的业务流程。此集成消除了 ETL 过程，使业务分析师、数据工程师和数据科学家能使用自助功能，并对操作数据运行准实时 BI、分析和机器学习管道。

### 任务 1：启用 Azure Synapse Link

1. 在 Azure 门户 (<https://portal.azure.com>) 中打开用于实验室环境的资源组。

2. 选择“**Azure Cosmos DB 帐户**”。

    ![Azure Cosmos DB 帐户已突出显示。](images/resource-group-cosmos.png "Azure Cosmos DB account")

3. 选择左侧菜单中的“**功能**”，然后选择“**Azure Synapse Link**”。

    ![将显示“功能”边栏选项卡。](images/cosmos-db-features.png "Features")

4. 选择“**启用**”。

    ![“启用”已突出显示。](images/synapse-link-enable.png "Azure Synapse Link")

    在使用分析存储创建 Azure Cosmos DB 容器之前，必须先启用 Azure Synapse Link。

5. 必须等待此操作完成才能继续，这可能需要一分钟时间。可选择 Azure“**通知**”图标查看状态。

    ![“启用 Synapse Link”进程正在运行。](images/notifications-running.png "Notifications")

    “启用 Synapse Link”成功完成后，其旁边会显示一个绿色对勾符号。

    ![操作成功完成。](images/notifications-completed.png "Notifications")

### 任务 2：创建一个新的 Azure Cosmos DB 容器

Tailwind Traders 有一个名为“**OnlineUserProfile01**”的 Azure Cosmos DB 容器。由于我们在创建该容器*后*启用了 Azure Synapse Link 功能，因此无法在该容器上启用分析存储。我们会创建一个新容器，使其具有相同的分区键并启用分析存储。

创建容器后，我们将创建一个新的 Synapse 管道，用于将数据从“**OnlineUserProfile01**”容器复制到新的容器。

1. 在左侧菜单中，选择“**数据资源管理器**”。

    ![已选择菜单项。](images/data-explorer-link.png "Data Explorer")

2. 选择“**新建容器**”。

    ![图中突出显示了按钮。](images/new-container-button.png "New Container")

3. 使用以下设置创建一个新的容器：
    - **数据库 ID**：使用现有“**CustomerProfile**”数据库。
    - **容器 ID**：输入 `UserProfileHTAP`
    - **分区键**：输入 `/userId`
    - **吞吐量**：选择“**自动缩放**”
    - **容器最大 RU/s**：输入 `4000`
    - **分析存储**：开

    ![已按照描述配置表单。](images/new-container.png "New container")

    在这里，我们将“**分区键**”值设置为“**userId**”，因为它是我们在查询中最常使用的字段，并包含相对较高的基数（唯一值的数量），这样能实现良好的分区性能。我们将吞吐量设置为自动缩放，且最大值为 4,000 个请求单位 (RU)。这意味着，该容器将至少通过分配获得 400 个 RU（最大值的 10%），并且会在缩放引擎检测到足够高的需求时纵向扩展至最大值 4,000 以保证增加吞吐量。最后，我们会在容器上启用**分析存储**，以充分利用 Synapse Analytics 中的混合事务/分析处理 (HTAP) 体系结构。

    让我们快速查看一下要复制到新容器中的数据。

4. 展开 **CustomerProfile** 数据库下的 **OnlineUserProfile01** 容器，然后选择“**项**”。选择其中一个文档并查看其内容。文档以 JSON 格式存储。

    ![将显示容器项。](images/existing-items.png "Container items")

5. 在左侧菜单中选择“**键**”。稍后将需要“**主键**”和 Cosmos DB 帐户名（在左上角），因此请让此选项卡保持打开状态。

    ![主建已突出显示。](images/cosmos-keys.png "Keys")

### 任务 3：创建并运行复制管道

拥有新的 Azure Cosmos DB 容器并启用分析存储后，接下来需要使用 Synapse Pipeline 复制现有容器的内容。

1. 在其他选项卡中打开 Synapse Studio (<https://web.azuresynapse.net/>)，然后导航到“**集成**”中心。

    ![“集成”菜单项已突出显示。](images/integrate-hub.png "Integrate hub")

2. 在“**+**”菜单中，选择“**管道**”。

    ![“新建管道”链接已突出显示。](images/new-pipeline.png "New pipeline")

3. 在“**活动**”下，展开“**移动和转换**”组，然后将“**复制数据**”活动拖到画布上。在“**属性**”边栏选项卡中，将“**名称**”设置为 **`Copy Cosmos DB Container`**。

    ![此时会显示新的复制活动。](images/add-copy-pipeline.png "Add copy activity")

4. 选择你添加到画布的新“**复制数据**”活动；并在画布下方的“**源**”选项卡上，选择“**asal400_customerprofile_cosmosdb**”源数据集。

    ![选择源。](images/copy-source.png "Source")

5. 选择“**接收器**”选项卡，然后选择“**+ 新建**”。

    ![选择接收器。](images/copy-sink.png "Sink")

6. 选择“**Azure Cosmos DB (SQL API)**”数据集类型，然后选择“**继续**”。

    ![已选择 Azure Cosmos DB。](images/dataset-type.png "New dataset")

7. 设置以下属性，然后单击“**确定**”：
    - **名称**：输入 `cosmos_db_htap`。
    - **链接服务**：选择“**asacosmosdb01**”。
    - **集合**：选择“**UserProfileHTAP/**”
    - **导入架构**：在“**导入架构**”下选择“**从连接/存储**”。

    ![已按照描述配置表单。](images/dataset-properties.png "Set properties")

8. 在刚添加的新接收器数据集下，请确保已选择“**插入**”写入行为。

    ![显示“接收器”选项卡。](images/sink-insert.png "Sink tab")

9. 选择“**全部发布**”和“**发布**”，以保存新管道。

    ![全部发布。](images/publish-all-1.png "Publish")

10. 在管道画布上方，依次选择“**添加触发器**”、“**立即触发**”。选择“**确定**”以触发运行。

    ![显示触发器菜单。](images/pipeline-trigger.png "Trigger now")

11. 导航到“**监视**”中心。

    ![“监视”中心。](images/monitor-hub.png "Monitor hub")

12. 选择“**管道运行**”并等待管道运行成功完成。可能需要多次选择“**刷新**”。

    ![管道运行显示为成功完成。](images/pipeline-run-status.png "Pipeline runs")

    > 此操作可能需要**大约 4 分钟**才能完成。

## 练习 2 - 使用 Apache Spark for Synapse Analytics 查询 Azure Cosmos DB

Tailwind Traders 希望使用 Apache Spark 对新的 Azure Cosmos DB 容器运行分析查询。在这一段中，我们将使用 Synapse Studio 的内置笔势快速创建一个 Synapse 笔记本，用于从启用了 HTAP 的容器中加载分析存储数据，同时不影响事务存储。

Tailwind Traders 正在尝试解决如何使用每个用户标识的首选产品列表，以及在他们的评论历史记录中任何匹配的产品 ID，以显示所有首选产品评论的列表。

### 任务 1：创建笔记本

1. 导航到“**数据**”中心。

    ![“数据”中心。](images/data-hub.png "Data hub")

2. 选择“**链接**”选项卡并展开“**Azure Cosmos DB**”部分（如果这部分不可见，请使用右上角的 **&#8635;** 按钮刷新 Synapse Studio），然后展开“**asacosmosdb01 (CustomerProfile)**”链接服务。右键单击“**UserProfileHTAP**”容器，选择“**新建笔记本**”，然后选择“**加载到 DataFrame**”。

    ![“新建笔记本”笔势已突出显示。](images/new-notebook.png "New notebook")

    请注意，我们创建的“**UserProfileHTAP**”容器的图标与其他容器的略有不同。这表示已启用分析存储。

3. 在新的笔记本中，在“**附加到**”下拉列表中选择 **SparkPool01** Spark 池。

    ![“附加到”下拉列表已突出显示。](images/notebook-attach.png "Attach the Spark pool")

4. 选择“**全部运行**”。

    ![显示新的笔记本，其中包括单元格 1 输出。](images/notebook-cell1.png "Cell 1")

    Spark 会话首次启动时需要几分钟时间。

    请注意，在单元格 1 的生成代码中，“**spark.read**”格式已设置为“**cosmos.olap**”。这表示 Synapse Link 将使用容器的分析存储。如果希望连接到事务存储，以便读取更改源中的数据或写入容器，则可改为使用“**cosmos.oltp**”。

    > **备注：** 不能写入分析存储，而只能从分析存储中读取数据。如果希望将数据加载到容器，则需要连接到事务存储。

    第一个选项配置 Azure Cosmos DB 链接服务的名称。第二个“选项”定义要从中读取数据的 Azure Cosmos DB 容器。

5. 选择运行的单元格下方的“**+ 代码**”按钮。此操作将在第一个代码单元格下方添加新的代码单元格。

6. DataFrame 包含我们不需要的额外列。让我们删除不需要的列，创建一个干净的 DataFrame 版本。若要实现这一点，请在新代码单元格中输入以下内容并运行它：

    ```python
    unwanted_cols = {'_attachments','_etag','_rid','_self','_ts','collectionType','id'}

    # Remove unwanted columns from the columns collection
    cols = list(set(df.columns) - unwanted_cols)

    profiles = df.select(cols)

    display(profiles.limit(10))
    ```

    输出现在只包含所需的列。请注意，“**preferredProducts**”和“**productReviews**”列包含子元素。展开某一行的值以查看它们。你可能在 Azure Cosmos DB 数据资源管理器中看到过“**UserProfiles01**”容器中的原始 JSON 格式。

    ![显示了单元格输出。](images/cell2.png "Cell 2 output")

7. 我们应了解需要处理多少条记录。若要实现这一点，请在新代码单元格中输入以下内容并运行它：

    ```python
    profiles.count()
    ```

    应会看到计数结果为 99,999。

8. 我们希望对每个用户都使用“**preferredProducts**”和“**productReviews**”列数组，并创建一个产品图表，这些产品来自他们的首选列表，与他们评论过的产品相匹配。为此，我们需要创建两个新的 DataFrame，使其包含这两列中的平展值，以便稍后我们可以联接它们。在新代码单元格中输入以下内容并“运行”它：

    ```python
    from pyspark.sql.functions import udf, explode

    preferredProductsFlat=profiles.select('userId',explode('preferredProducts').alias('productId'))
    productReviewsFlat=profiles.select('userId',explode('productReviews').alias('productReviews'))
    display(productReviewsFlat.limit(10))
    ```

    在此单元格中，我们导入了一个特殊的 PySpark [explode ](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode) 函数，它为数组的每个元素返回一个新行。此函数有助于平展“**preferredProducts**”和“**productReviews**”列，以提高可读性或方便查询。

    ![单元格输出。](images/cell4.png "Cell 4 output")

    观察单元格输出，其中显示了“**productReviewFlat**” DataFrame 内容。我们会看到一个新的“**productReviews**”列，该列包含我们希望与用户的首选产品列表匹配的“**productId**”，以及我们希望显示或保存的“**reviewText**”。

9. 我们来看看“**preferredProductsFlat**” DataFrame 内容。若要实现这一点，请在新单元格中输入以下内容并**运行**它：

    ```python
    display(preferredProductsFlat.limit(20))
    ```

    ![单元格输出。](images/cell5.png "Cell 5 results")

    由于我们在首选产品数组上使用了“**explode**”函数，我们已将列值平展到“**userId**”和“**productId**”行，并按用户排序。

10. 现在，我们需要进一步平展“**productReviewFlat**” DataFrame 内容，以提取“**productReviews.productId**”和“**productReviews.reviewText**”字段并为每个数据组合创建新行。若要实现这一点，请在新代码单元格中输入以下内容并运行它：

    ```python
    productReviews = (productReviewsFlat.select('userId','productReviews.productId','productReviews.reviewText')
        .orderBy('userId'))

    display(productReviews.limit(10))
    ```

    在输出中，请注意现在每个 `userId` 都有多行。

    ![单元格输出。](images/cell6.png "Cell 6 results")

11. 最后一步是联接“**userId**”和“**productId**”值上的“**preferredProductsFlat**”和“**productReviews**” DataFrame，以创建首选产品评论图表。若要实现这一点，请在新代码单元格中输入以下内容并运行它：

    ```python
    preferredProductReviews = (preferredProductsFlat.join(productReviews,
        (preferredProductsFlat.userId == productReviews.userId) &
        (preferredProductsFlat.productId == productReviews.productId))
    )

    display(preferredProductReviews.limit(100))
    ```

    > **备注**：可单击表视图中的列标题来对结果集进行排序。

    ![单元格输出。](images/cell7.png "Cell 7 results")

12. 使用笔记本右上角的“**停止会话**”按钮来停止笔记本会话。然后关闭笔记本，放弃更改。

## 练习 3 - 使用 Azure Synapse Analytics 中的无服务器 SQL 池查询 Azure Cosmos DB

Tailwind Traders 希望使用 T-SQL 浏览 Azure Cosmos DB 分析存储。理想情况下，他们可以创建多个视图，这些视图随后可用于联接其他分析存储容器、Data Lake 中的文件或可被 Power BI 等外部工具使用。

### 任务 1：创建新的 SQL 脚本

1. 导航到“**开发**”中心。

    ![“开发”中心。](images/develop-hub.png "Develop hub")

2. 在“+”菜单中，选择“**SQL 脚本**”。

    ![突出显示了“SQL 脚本”按钮。](images/new-script.png "SQL script")

3. 脚本打开后，在右侧的“**属性**”窗格中，将“**名称**”更改为 `User Profile HTAP`。然后使用“**属性**”按钮关闭窗格。

    ![显示“属性”窗格。](images/new-script-properties.png "Properties")

4. 验证是否选择了无服务器 SQL 池（**内置**）。

    ![无服务器 SQL 池已选中。](images/built-in-htap.png "Built-in")

5. 粘贴下面的 SQL 查询。在 OPENROWSET 语句中，将“**YOUR_ACCOUNT_NAME**”替换为 Azure Cosmos DB 帐户名，将“**YOUR_ACCOUNT_KEY**”替换为 Azure 门户中“**密钥**”页面中的 Azure Cosmos DB 主密钥（应该仍会在另一个选项卡中打开）。

    ```sql
    USE master
    GO

    IF DB_ID (N'Profiles') IS NULL
    BEGIN
        CREATE DATABASE Profiles;
    END
    GO

    USE Profiles
    GO

    DROP VIEW IF EXISTS UserProfileHTAP;
    GO

    CREATE VIEW UserProfileHTAP
    AS
    SELECT
        *
    FROM OPENROWSET(
        'CosmosDB',
        N'account=YOUR_ACCOUNT_NAME;database=CustomerProfile;key=YOUR_ACCOUNT_KEY',
        UserProfileHTAP
    )
    WITH (
        userId bigint,
        cartId varchar(50),
        preferredProducts varchar(max),
        productReviews varchar(max)
    ) AS profiles
    CROSS APPLY OPENJSON (productReviews)
    WITH (
        productId bigint,
        reviewText varchar(1000)
    ) AS reviews
    GO
    ```

6. 使用“**运行**”按钮运行查询，会：
    - 创建名为“**Profiles**”的新无服务器 SQL 池数据库（如果不存在）
    - 将数据库上下文更改为 **Profiles** 数据库。
    - 删除“**UserProfileHTAP**”视图（如果存在）。
    - 创建名为“**UserProfileHTAP**”的 SQL 视图。
    - 使用 OPENROWSET 语句将数据源类型设置为“**CosmosDB**”，设置帐户详细信息并指出我们希望对名为“**UserProfileHTAP**”的 Azure Cosmos DB 分析存储容器创建视图。
    - 匹配 JSON 文件中的属性名并应用合适的 SQL 数据类型。请注意，我们已将“**preferredProducts**”和“**productReviews**”字段设置为“**varchar(max)**”。这是因为这两个属性都包含 JSON 格式的数据。
    - 由于 JSON 文档中的“**productReviews**”属性包含嵌套子数组，因此脚本需要将文档中的属性与数组的所有元素联接起来。借助 Synapse SQL，我们可以对嵌套数组应用 OPENJSON 函数，从而平展嵌套结构。平展“**productReviews**”中的值，就像我们在前面的 Synapse 笔记本中使用 Python **explode** 函数执行的操作一样。

7. 导航到“**数据**”中心。

    ![“数据”中心。](images/data-hub.png "Data hub")

8. 选择“**工作区**”选项卡并展开“**数据库**”组。展开 **Profiles** 按需 SQL 数据库（如果没有在列表中看到它，请刷新“**数据库**”列表）。展开“**视图**”，然后右键单击“**UserProfileHTA**”视图，选择“**新建 SQL 脚本**”，然后“**选择前 100 行**”。

    ![“选择前 100 行”查询选项已突出显示。](images/new-select-query.png "New select query")

9. 确保脚本已连接到**内置** SQL 池，然后运行查询并查看结果。

    ![视图结果已显示。](images/select-htap-view.png "Select HTAP view")

    视图中包含“**preferredProducts**”和“**productReviews**”字段，它们都包含 JSON 格式的值。注意视图中的 CROSS APPLY OPENJSON 语句如何通过将“**productId**”和“**reviewText**”值提取到新字段，成功平展“**productReviews**”字段中的嵌套子数组值。
