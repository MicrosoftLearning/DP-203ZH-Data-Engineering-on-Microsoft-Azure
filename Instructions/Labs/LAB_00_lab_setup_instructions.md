---
lab:
    title: '使用预安装的虚拟机设置实验室环境'
    module: '模块 0'
---

# 模块 0 - 使用预安装的虚拟机设置实验室环境

以下说明可帮助学员为后面的模块准备实验室环境。开始模块 1 之前，请先阅读这些说明。

**完成所需时间**：执行以下步骤并启动自动设置脚本大约需要 5 分钟。脚本可能需要一个小时或更长时间才能完成。

> **备注**：这些说明将在为课程提供的预安装的虚拟机中使用。

## 要求

在开始设置之前，你需要一个能够创建 Azure Synapse 工作区的 Azure 帐户。

> **重要注意事项（如果使用 Azure Pass 订阅）**
>
> 如果所使用的帐户之前兑换过 Azure Pass 订阅但订阅已过期，则该帐户可能与多个同名的 Azure 订阅相关联（*Azure Pass - 赞助*）。在开始设置步骤之前，执行以下步骤来确保只启用时间上最近的具有此名称的*活动*订阅：
>
> 1. 打开 Azure 门户 (`https://portal.azure.com`)，使用与你的订阅关联的帐户登录。
> 2. 在页面顶部的门户工具栏中，选择“**目录和订阅**”按钮。
> 3. 在“**默认订阅筛选器**”下拉列表中，*取消选择*任何“**（禁用的） Azure Pass - 赞助**”订阅，并确保<u>仅</u>选中所需的活动“**Azure Pass - 赞助**”订阅。

## 设置步骤

执行以下任务来为实验室准备环境。

1. 使用 Windows **搜索**框搜索 **Windows PowerShell**，然后以管理员身份运行它。

    > **备注**：确保运行的是 **Windows Powershell**，而 <u>不是</u> Windows Powershell ISE，并确保以管理员身份运行它。

2. 在 Windows PowerShell 中，运行以下命令下载所需的课程文件。这可能需要几分钟时间。

    ```
    mkdir c:\dp-203

    cd c:\dp-203

    git clone https://github.com/microsoftlearning/dp-203-data-engineer.git data-engineering-ilt-deployment
    ```

3. 在 Windows PowerShell 中，运行以下命令设置执行策略，以便运行本地 PowerShell 脚本文件：

    ```
    Set-ExecutionPolicy Unrestricted
    ```

    > **备注**：如果收到提示，指示正在从不受信任的存储库安装模块，请输入“**A**”以选择“*全部确认*”选项。

4. 在 Windows PowerShell 中，使用以下命令将目录更改为包含自动化脚本的文件夹。

    ```
    cd C:\dp-203\data-engineering-ilt-deployment\Allfiles\00\artifacts\environment-setup\automation\
    ```
    
5. 在 Windows PowerShell 中，输入以下命令运行设置脚本：

    ```
    .\dp-203-setup.ps1
    ```

6. 当系统提示你登录 Azure 时，浏览器将打开；请使用你的凭据登录。登录后，你可以关闭浏览器并返回 Windows PowerShell，它将显示你可以访问的 Azure 订阅。

7. 出现提示时，请再次登录你的 Azure 帐户（这是必需的，以便脚本可以管理 Azure 订阅中的资源 - 请确保使用与以前相同的凭据）。

8. 如果有多个 Azure 订阅，请在出现提示时，通过在订阅列表中输入订阅编号来选择要在实验室中使用的订阅。

9. 出现提示时，为 SQL 数据库输入一个合适的复杂密码（请记住这个密码，以便以后使用）。

脚本运行时，你的讲师将展示本课程的第一个模块。当要开始第一个实验时，环境应该已经准备好了。

> **备注**：完成该脚本需要 45-60 分钟。该脚本将使用随机生成的名称创建 Azure 资源。如果脚本出现“停滞”（10 分钟内没有显示新的信息），按 ENTER 并查看是否有任何错误消息（通常脚本会顺利运行而不会出现问题）。  在极少情况下，可能会出现资源名重复、存在对随机选择的区域中的特定资源的容量限制，或者可能发生短暂的网络问题，这些问题可能导致出错。如果出现这些情况，可使用 Azure 门户删除脚本创建的 **data-engineering-synapse-*xxxxxx*** 资源组，然后重新运行脚本。
>
> 如果显示一个错误，指示需要为 Azure pass 订阅提供**租户 Id**，请确保已按照上面“**要求**”部分中的说明，仅启用了要使用的 Azure Pass 订阅。
