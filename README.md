# SillyTavern 独立后台生成

在服务器上完成已提交的回复，手机关闭页面后，重新打开同一聊天取回结果。

这是从零编写的前端扩展和服务端插件。没有引用、复制或要求安装柏宝箱、柏宝库、Luker，也不修改它们或 SillyTavern 核心文件。只通过原生聊天事件、保存接口和生成路由适配 SillyTavern。干净实例验收时仅安装本项目的两个目录。

目前验证环境为 **SillyTavern 1.19.0、Node.js 24、Linux ARM64**。原生路由适配会检查结构，其他版本需要重新验收。没有 npm 依赖，也不需要编译前端。

## 使用范围

- 普通单角色主回复，OpenAI 或 Custom Chat Completion 接口，单条候选；支持流式和非流式响应。
- 支持单一纯正文封装：`output_everything_here/content`、`game_content/context`、`emit_complete_response/content`。原始工具响应保持不变，服务器另外保存其正文。
- 静默辅助生成、群聊、继续、重生成、多候选以及需要真正执行函数的工具请求使用酒馆原生前台流程。这些场景不能依赖本插件关页续跑。
- 前端显示“已交给服务器，可以离开”之后再退出；提示词还没组装完或请求还没被接收时退出，不能保证任务已提交。

收到上游错误就报告失败；本插件不会替换模型、自动重试模型请求或掩盖上游权限、额度与首字等待问题。

## 保存和恢复

服务器先把接管记录落盘，再启动原生生成。浏览器断开只停止结果传输；点击“停止生成”才请求取消。任务编号可重复查询、重复提交不重复调用模型。

任务文件只记录聊天目标、基线摘要、状态与结果；不保存生成请求、提示词、请求头、Cookie 或 API 密钥。已完成结果可跨服务器重启保留。重启时尚未完成的任务标为中断，**不会自动重新调用模型续跑**。

返回聊天后，扩展先检查已有任务标记，再核对聊天消息和元数据。相符才通过原生界面插入、保存，服务端确认实际文件已有对应消息后才确认消费。发生变化时保留结果供查看、复制或明确放弃，避免用旧页面覆盖已发生的编辑。未确认受理的请求可在面板“确认未受理并解除等待”，服务器先建立禁止迟到重发的记录，再解除等待。

每用户最多32个未消费任务、4个活跃任务、128MiB记录；单条响应流最多2MiB、单记录最多6MiB，接管输入最多16MiB，单次生成最长15分钟。已消费记录会压缩成小型防重发记录，保留至请求失效后清理；未消费结果不因到期而偷偷删除。清理在后续使用或服务启动时执行，不运行定时AI任务。

仍有明确边界：原生聊天保存没有强制版本锁，多标签恰好同时写入不能保证严格互斥。恢复不会重放 `MESSAGE_RECEIVED`，因此不会自动补跑 MVU、酒馆助手脚本或多轮工具链。真实手机长篇锁屏、省电策略和全部角色预设尚未全面验收。建议同一聊天只在一个页面生成或编辑。

## 安装

先取得本项目，然后预览要安装的文件：

```sh
git clone https://github.com/Foreseer299/sillytavern-background-jobs.git
cd sillytavern-background-jobs
python3 scripts/install.py install --root /path/to/SillyTavern
python3 scripts/install.py install --root /path/to/SillyTavern --apply
```

默认安装到 `plugins/background-jobs` 与 `public/scripts/extensions/third-party/background-jobs`。Docker 分离挂载可指定实际持久目录：

```sh
python3 scripts/install.py install --root /path/to/app \
  --server-root /path/to/plugins/background-jobs \
  --extension-root /path/to/extensions/background-jobs --apply
```

在酒馆正在使用的 `config.yaml` 中确保 `enableServerPlugins: true`，确认没有进行中的生成任务后，只重启该酒馆进程。刷新浏览器，在扩展设置中打开“独立后台生成”。如果另有后台接管扩展，请关闭其后台接管功能，其他功能可继续保留；不要同时让两套功能接管同一个生成请求。

安装器默认只预览，不改配置、用户资料或其他插件，也不重启服务。遇到已有未知文件会拒绝覆盖。前端默认关闭，需明确启用。任务目录位于各用户自己的数据目录下的 `background-jobs/`，目录权限0700、记录0600，不应发布到公开仓库。

## 卸载或回退

确认没有进行中的任务，使用安装时相同的目录参数：

```sh
python3 scripts/install.py uninstall --root /path/to/SillyTavern
python3 scripts/install.py uninstall --root /path/to/SillyTavern --apply
```

卸载只删除安装记录中仍匹配哈希的本项目代码；有后续编辑则停止。聊天、已保存任务和额外文件保留。随后重启该酒馆并刷新页面；如果切回其他后台方案，再单独启用其后台开关。安装记录默认是应用根目录的 `.background-jobs-install.json`，用自定义 `--receipt` 安装时卸载也需指定同一路径。

## 测试与来源

```sh
node --test tests/*.mjs
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

测试只使用合成数据，不自动请求真实模型。真实 PG、隔离浏览器及生产接口验收结果见 [VALIDATION.md](VALIDATION.md)。

项目代码使用 [MIT 许可证](LICENSE)。依赖安装者已有的 SillyTavern 及其许可；本仓库不附带酒馆源码。集成依据为 [官方服务端插件接口](https://docs.sillytavern.app/for-contributors/server-plugins/)和[官方前端扩展接口](https://docs.sillytavern.app/for-contributors/writing-extensions/)，以及已安装版本的原生接口核验。

之前的[柏宝箱兼容补丁](https://github.com/Foreseer299/sillytavern-background-compat)继续保留。该项目是另一种方案，本独立实现没有导入其代码或补丁。
