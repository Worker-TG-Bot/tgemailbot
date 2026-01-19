~~# 待优化~~
1. 特殊字符等未剔除，如：```&#8202;```
2. 链接未获取渲染成超链接
3. Bot 发送的整条消息（包含标题、发件人、日期和正文）都应该使用 Markdown 语法进行排版渲染，这样才能保证加粗、链接、分割线等视觉效果正确。
也可以使用HTML
```
Telegram 的 HTML 解析器是一个“精简版”，只支持以下标签：
​加粗：<b>bold</b> 或 <strong>bold</strong>
​斜体：<i>italic</i> 或 <em>italic</em>
​下划线：<u>underline</u> 或 <ins>underline</ins>
​删除线：<s>strike</s> 或 <del>strike</del>
​超链接：<a href="http://www.google.com">文字</a>
​等宽代码：<code>inline code</code>
​代码块：<pre>pre-formatted block</pre>
​隐藏内容（剧透）：<span class="tg-spoiler">spoiler</span>
​注意：不支持 <div>, <span> (除剧透外), <img>, <br>, <table> 等。
注意：​在 Telegram Bot API 中，如果不显式指定 parse_mode： "parse_mode": "HTML"，服务器会默认将 text 视为纯文本。
```
4. 容易掉线（需要重新授权），掉线后账号状态没有更新和通知