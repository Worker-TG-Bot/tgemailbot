变成使用cloudflare的ai进行总结，不再手动清洗HTML
在wrangler.toml里面添加：
```
[ai]
binding = "AI"
```