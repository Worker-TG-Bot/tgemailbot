/**
 * Gmail Telegram Bot - Cloudflare Worker (v2.3)
 * 功能：通过 Telegram Bot 安全访问 Gmail 邮箱
 * 特性：多账户、分页浏览、实时推送、网页预览、批量操作
 */

const GMAIL_SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.modify'
].join(' ');

const PAGE_SIZE = 5;
const MAX_CONTENT_LENGTH = 3500;
const PREVIEW_LENGTH = 300;

// ==================== 主入口 ====================
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // 设置 webhook
      if (path === '/setup') {
        return await handleSetup(url, env);
      }
      
      // OAuth 回调
      if (path === '/oauth/callback') {
        return await handleOAuthCallback(request, env);
      }
      // 隐私政策
      if (path === '/privacy') {
        return new Response(getPrivacyPage(), {
          headers: { 'Content-Type': 'text/html; charset=utf-8' }
        });
      }
      
      // 服务条款
      if (path === '/terms') {
        return new Response(getTermsPage(), {
          headers: { 'Content-Type': 'text/html; charset=utf-8' }
        });
      }
      // Google 站点验证
      if (path === '/googlefef45634f33fc82b.html') {
        return new Response('google-site-verification: googlefef45634f33fc82b.html', {
          headers: { 'Content-Type': 'text/html; charset=utf-8' }
        });
      }
      
      // 邮件网页预览
      if (path.startsWith('/mail/')) {
        return await handleMailView(path, env);
      }
      
      // Pub/Sub 推送
      if (path === '/pubsub/push') {
        const secret = url.searchParams.get('secret');
        if (secret !== env.BOT_SECRET) {
          return new Response('Unauthorized', { status: 401 });
        }
        const message = await request.json();
        ctx.waitUntil(handlePubSubPush(message, env));
        return new Response('OK');
      }
      
      // Telegram webhook
      if (path === '/webhook' && request.method === 'POST') {
        const secret = url.searchParams.get('secret');
        if (secret !== env.BOT_SECRET) {
          return new Response('Unauthorized', { status: 401 });
        }
        const update = await request.json();
        ctx.waitUntil(handleTelegramUpdate(update, env, url.origin));
        return new Response('OK');
      }

      return new Response(getHomePage(), {
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
      });
    } catch (error) {
      console.error('Error:', error);
      return new Response('Error: ' + error.message, { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(renewAllWatches(env));
  }
};

// ==================== 统一时间函数 ====================
function toBeijingTime(date) {
  const d = typeof date === 'string' ? new Date(date) : date;
  return new Date(d.getTime() + 8 * 60 * 60 * 1000);
}

function formatDate(dateStr, format = 'short') {
  try {
    const d = toBeijingTime(dateStr);
    const year = d.getUTCFullYear();
    const month = d.getUTCMonth() + 1;
    const day = d.getUTCDate();
    const hours = String(d.getUTCHours()).padStart(2, '0');
    const minutes = String(d.getUTCMinutes()).padStart(2, '0');
    const seconds = String(d.getUTCSeconds()).padStart(2, '0');
    
    switch (format) {
      case 'chinese':
        const weekdays = ['日', '一', '二', '三', '四', '五', '六'];
        const weekday = weekdays[d.getUTCDay()];
        return `${year}年${month}月${day}日 星期${weekday} ${hours}:${minutes} (北京时间)`;
      case 'full':
        return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
      case 'short':
      default:
        return `${month}/${day} ${hours}:${minutes}`;
    }
  } catch {
    return dateStr;
  }
}

function getTodayTimestamp() {
  const now = Date.now();
  const offset = 8 * 60 * 60 * 1000;
  const beijingMs = now + offset;
  const dayMs = 24 * 60 * 60 * 1000;
  const beijingMidnight = beijingMs - (beijingMs % dayMs);
  return Math.floor((beijingMidnight - offset) / 1000);
}

// ==================== Telegram API ====================
async function sendTelegram(token, method, data) {
  const resp = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  return resp.json();
}

function getMainKeyboard() {
  return {
    keyboard: [
      [{ text: '📬 收件箱' }, { text: '📅 今日' }, { text: '⭐ 星标' }],
      [{ text: '🔍 搜索' }, { text: '📊 统计' }, { text: '✅ 全已读' }],
      [{ text: '👤 账户管理' }, { text: '⚙️ 设置' }]
    ],
    resize_keyboard: true,
    persistent: true
  };
}

// ==================== Token 管理 ====================
async function getActiveAccount(userId, env) {
  const email = await env.USER_TOKENS.get(`active:${userId}`);
  if (!email) return null;
  
  const tokenRaw = await env.USER_TOKENS.get(`token:${userId}:${email}`);
  if (!tokenRaw) {
    // Token不存在，通知用户并清理
    await notifyTokenExpired(userId, email, env);
    await cleanupExpiredAccount(userId, email, env);
    return null;
  }
  
  try {
    const token = JSON.parse(tokenRaw);
    token.email = email;
    
    if (Date.now() > token.expiry - 60000) {
      const refreshed = await refreshToken(token.refresh_token, env);
      if (refreshed) {
        token.access_token = refreshed.access_token;
        token.expiry = Date.now() + refreshed.expires_in * 1000;
        await env.USER_TOKENS.put(`token:${userId}:${email}`, JSON.stringify(token));
      } else {
        // Refresh失败，通知用户并清理
        await notifyTokenExpired(userId, email, env);
        await cleanupExpiredAccount(userId, email, env);
        return null;
      }
    }
    
    return token;
  } catch {
    // 解析失败，通知用户并清理
    await notifyTokenExpired(userId, email, env);
    await cleanupExpiredAccount(userId, email, env);
    return null;
  }
}

async function notifyTokenExpired(userId, email, env) {
  // 检查是否已经通知过（避免重复通知）
  const notifiedKey = `notified:${userId}:${email}`;
  const alreadyNotified = await env.USER_TOKENS.get(notifiedKey);
  
  if (!alreadyNotified) {
    await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
      chat_id: userId,
      text: `⚠️ <b>账户授权已过期</b>\n\n📧 ${escapeHtml(email)}\n\n请重新授权以继续使用。\n\n点击 <b>👤 账户管理</b> → <b>➕ 添加账户</b> 重新登录。`,
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [[
          { text: '🔐 重新授权', callback_data: 'add' }
        ]]
      }
    });
    
    // 标记已通知，24小时后过期（避免重复通知）
    await env.USER_TOKENS.put(notifiedKey, 'true', { expirationTtl: 86400 });
  }
}

async function cleanupExpiredAccount(userId, email, env) {
  // 删除过期的token
  await env.USER_TOKENS.delete(`token:${userId}:${email}`);
  
  // 从账户列表中移除
  const accounts = await getAccountList(userId, env);
  const index = accounts.indexOf(email);
  if (index > -1) {
    accounts.splice(index, 1);
    await env.USER_TOKENS.put(`accounts:${userId}`, JSON.stringify(accounts));
  }
  
  // 如果是当前活动账户，切换到其他账户或清除
  const active = await env.USER_TOKENS.get(`active:${userId}`);
  if (active === email) {
    if (accounts.length > 0) {
      await env.USER_TOKENS.put(`active:${userId}`, accounts[0]);
    } else {
      await env.USER_TOKENS.delete(`active:${userId}`);
    }
  }
  
  // 清理推送设置
  await env.USER_TOKENS.delete(`push:${userId}:${email}`);
}

async function refreshToken(refreshToken, env) {
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GOOGLE_CLIENT_ID,
      client_secret: env.GOOGLE_CLIENT_SECRET,
      refresh_token: refreshToken,
      grant_type: 'refresh_token'
    })
  });
  const data = await resp.json();
  return data.access_token ? data : null;
}

async function getAccountList(userId, env) {
  const raw = await env.USER_TOKENS.get(`accounts:${userId}`);
  try {
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

// ==================== 邮件ID映射 ====================
async function storeMailIds(userId, mails, env) {
  const mapping = {};
  mails.forEach((mail, index) => {
    mapping[index] = mail.id;
  });
  // 邮件ID映射1小时后过期
  await env.USER_TOKENS.put(`mailmap:${userId}`, JSON.stringify(mapping), { expirationTtl: 3600 });
  return mapping;
}

async function getMailId(userId, index, env) {
  const raw = await env.USER_TOKENS.get(`mailmap:${userId}`);
  if (!raw) return null;
  try {
    const mapping = JSON.parse(raw);
    return mapping[index] || null;
  } catch {
    return null;
  }
}

// ==================== Setup ====================
async function handleSetup(url, env) {
  const secret = url.searchParams.get('secret');
  if (secret !== env.BOT_SECRET) {
    return new Response('Unauthorized', { status: 401 });
  }

  const webhookUrl = `${url.origin}/webhook?secret=${env.BOT_SECRET}`;
  await sendTelegram(env.BOT_TOKEN, 'setWebhook', { url: webhookUrl });
  await sendTelegram(env.BOT_TOKEN, 'deleteMyCommands', {});

  return new Response(`✅ 设置完成！Webhook: ${webhookUrl}`);
}

// ==================== OAuth ====================
async function handleOAuthCallback(request, env) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  const error = url.searchParams.get('error');

  if (error) {
    return new Response(getResultPage(false, error), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }

  if (!code || !state) {
    return new Response(getResultPage(false, '参数缺失'), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }

  let stateData;
  try {
    stateData = JSON.parse(atob(state));
  } catch {
    return new Response(getResultPage(false, 'State 无效'), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }

  const { userId, nonce } = stateData;
  const storedNonce = await env.USER_TOKENS.get(`nonce:${userId}`);
  
  if (!storedNonce || storedNonce !== nonce) {
    return new Response(getResultPage(false, '授权已过期，请重新登录'), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }

  const tokenResp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      code,
      client_id: env.GOOGLE_CLIENT_ID,
      client_secret: env.GOOGLE_CLIENT_SECRET,
      redirect_uri: `${url.origin}/oauth/callback`,
      grant_type: 'authorization_code'
    })
  });
  
  const tokenData = await tokenResp.json();
  
  if (!tokenData.access_token) {
    return new Response(getResultPage(false, tokenData.error_description || tokenData.error), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }

  const profileResp = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/profile', {
    headers: { Authorization: `Bearer ${tokenData.access_token}` }
  });
  const profile = await profileResp.json();
  const email = profile.emailAddress;

  await env.USER_TOKENS.put(`token:${userId}:${email}`, JSON.stringify({
    access_token: tokenData.access_token,
    refresh_token: tokenData.refresh_token,
    expiry: Date.now() + tokenData.expires_in * 1000
  }));

  const accounts = await getAccountList(userId, env);
  if (!accounts.includes(email)) {
    accounts.push(email);
    await env.USER_TOKENS.put(`accounts:${userId}`, JSON.stringify(accounts));
  }

  await env.USER_TOKENS.put(`active:${userId}`, email);
  await env.USER_TOKENS.delete(`nonce:${userId}`);

  await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
    chat_id: userId,
    text: `✅ 账户绑定成功！\n\n📧 ${email}\n\n点击下方按钮开始使用`,
    reply_markup: getMainKeyboard()
  });

  return new Response(getResultPage(true, email), {
    headers: { 'Content-Type': 'text/html; charset=utf-8' }
  });
}

// ==================== 邮件网页预览 ====================
async function handleMailView(path, env) {
  const token = path.replace('/mail/', '');
  const mailData = await env.USER_TOKENS.get(`view:${token}`);
  
  if (!mailData) {
    return new Response(getExpiredPage(), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
      status: 404
    });
  }

  try {
    const { userId, mailId, email } = JSON.parse(mailData);
    
    const tokenRaw = await env.USER_TOKENS.get(`token:${userId}:${email}`);
    if (!tokenRaw) {
      return new Response(getExpiredPage(), {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
        status: 401
      });
    }

    let tokenInfo = JSON.parse(tokenRaw);
    
    if (Date.now() > tokenInfo.expiry - 60000) {
      const refreshed = await refreshToken(tokenInfo.refresh_token, env);
      if (refreshed) {
        tokenInfo.access_token = refreshed.access_token;
        tokenInfo.expiry = Date.now() + refreshed.expires_in * 1000;
        await env.USER_TOKENS.put(`token:${userId}:${email}`, JSON.stringify(tokenInfo));
      } else {
        return new Response(getExpiredPage(), {
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
          status: 401
        });
      }
    }

    const resp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${mailId}?format=full`,
      { headers: { Authorization: `Bearer ${tokenInfo.access_token}` } }
    );

    if (!resp.ok) {
      return new Response(getExpiredPage(), {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
        status: 404
      });
    }

    const mail = await resp.json();
    return new Response(renderMailPage(mail), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });

  } catch (e) {
    return new Response(getExpiredPage(), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
      status: 500
    });
  }
}

function renderMailPage(mail) {
  const headers = mail.payload?.headers || [];
  const getHeader = (n) => headers.find(h => h.name.toLowerCase() === n.toLowerCase())?.value || '';
  
  const from = getHeader('From');
  const to = getHeader('To');
  const subject = getHeader('Subject') || '(无主题)';
  const date = formatDate(getHeader('Date'), 'chinese');

  let htmlContent = findBody(mail.payload, 'text/html');
  let textContent = findBody(mail.payload, 'text/plain');
  
  let body = '';
  if (htmlContent) {
    try {
      const base64 = htmlContent.replace(/-/g, '+').replace(/_/g, '/');
      body = decodeBase64(base64);
    } catch {
      body = '<p>无法解析邮件内容</p>';
    }
  } else if (textContent) {
    try {
      const base64 = textContent.replace(/-/g, '+').replace(/_/g, '/');
      const text = decodeBase64(base64);
      body = '<pre style="white-space:pre-wrap;word-wrap:break-word;font-family:inherit;">' + 
             text.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</pre>';
    } catch {
      body = '<p>无法解析邮件内容</p>';
    }
  } else {
    body = '<p>无邮件内容</p>';
  }

  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${subject.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; position: sticky; top: 0; z-index: 100; box-shadow: 0 2px 10px rgba(0,0,0,0.2); }
    .header h1 { font-size: 18px; font-weight: 600; margin-bottom: 12px; word-break: break-word; }
    .meta { font-size: 13px; opacity: 0.9; }
    .meta-row { margin: 4px 0; display: flex; }
    .meta-label { width: 50px; opacity: 0.7; }
    .meta-value { flex: 1; word-break: break-all; }
    .content { background: white; margin: 16px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); overflow: hidden; }
    .content-inner { padding: 20px; overflow-x: auto; }
    .content-inner img { max-width: 100%; height: auto; }
    .content-inner a { color: #667eea; }
    .content-inner table { max-width: 100%; }
    .footer { text-align: center; padding: 20px; color: #999; font-size: 12px; }
    .back-btn { display: inline-block; margin-top: 10px; padding: 8px 20px; background: #667eea; color: white; text-decoration: none; border-radius: 20px; font-size: 14px; }
    @media (max-width: 600px) {
      .header { padding: 16px; }
      .header h1 { font-size: 16px; }
      .content { margin: 12px; }
      .content-inner { padding: 16px; }
    }
  </style>
</head>
<body>
  <div class="header">
    <h1>${subject.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</h1>
    <div class="meta">
      <div class="meta-row"><span class="meta-label">发件人</span><span class="meta-value">${from.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</span></div>
      <div class="meta-row"><span class="meta-label">收件人</span><span class="meta-value">${to.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</span></div>
      <div class="meta-row"><span class="meta-label">时间</span><span class="meta-value">${date}</span></div>
    </div>
  </div>
  <div class="content">
    <div class="content-inner">${body}</div>
  </div>
  <div class="footer">
    <p>此链接 1 小时内有效</p>
    <a href="tg://resolve" class="back-btn">返回 Telegram</a>
  </div>
</body>
</html>`;
}

function getExpiredPage() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>链接已过期</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: linear-gradient(135deg, #1a1a2e, #16213e); color: white; min-height: 100vh; display: flex; align-items: center; justify-content: center; margin: 0; }
    .box { text-align: center; padding: 40px; }
    .icon { font-size: 64px; margin-bottom: 20px; }
    h1 { margin-bottom: 10px; }
    p { opacity: 0.7; margin-bottom: 20px; }
    .btn { display: inline-block; padding: 12px 30px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; text-decoration: none; border-radius: 25px; font-weight: 600; }
  </style>
</head>
<body>
  <div class="box">
    <div class="icon">⏰</div>
    <h1>链接已过期</h1>
    <p>此邮件预览链接已失效，请返回 Telegram 重新生成</p>
    <a href="tg://resolve" class="btn">返回 Telegram</a>
  </div>
</body>
</html>`;
}

function decodeBase64(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return new TextDecoder('utf-8').decode(bytes);
}

// ==================== 生成预览链接 ====================
async function generateViewLink(userId, mailId, email, env) {
  const origin = await env.USER_TOKENS.get('origin');
  const token = crypto.randomUUID();
  
  await env.USER_TOKENS.put(`view:${token}`, JSON.stringify({
    userId,
    mailId,
    email
  }), { expirationTtl: 3600 });
  
  return `${origin}/mail/${token}`;
}

// ==================== Telegram 更新处理 ====================
async function handleTelegramUpdate(update, env, origin) {
  await env.USER_TOKENS.put('origin', origin);

  if (update.callback_query) {
    await handleCallback(update.callback_query, env);
  } else if (update.message?.text) {
    await handleMessage(update.message, env);
  }
}

async function handleMessage(message, env) {
  const chatId = message.chat.id;
  const userId = String(message.from.id);
  const text = message.text.trim();

  const todayTs = getTodayTimestamp();

  const handlers = {
    '/start': () => sendWelcome(chatId, userId, env),
    '🏠 主菜单': () => sendWelcome(chatId, userId, env),
    '📬 收件箱': () => sendMailList(chatId, userId, 'in:inbox', null, null, env),
    '📅 今日': () => sendMailList(chatId, userId, `after:${todayTs}`, null, null, env),
    '⭐ 星标': () => sendMailList(chatId, userId, 'is:starred', null, null, env),
    '🔍 搜索': () => sendSearchHelp(chatId, env),
    '📊 统计': () => sendStats(chatId, userId, null, env),
    '✅ 全已读': () => markAllRead(chatId, userId, env),
    '👤 账户管理': () => sendAccountManager(chatId, userId, null, env),
    '⚙️ 设置': () => sendSettings(chatId, userId, env)
  };

  if (handlers[text]) {
    await handlers[text]();
    return;
  }

  if (text.startsWith('搜索 ') || text.startsWith('/search ')) {
    const query = text.replace(/^(搜索 |\/search )/, '').trim();
    if (query) {
      await sendMailList(chatId, userId, query, null, null, env);
    }
    return;
  }

  await sendWelcome(chatId, userId, env);
}

// ==================== 欢迎消息 ====================
async function sendWelcome(chatId, userId, env) {
  const accounts = await getAccountList(userId, env);
  const active = await env.USER_TOKENS.get(`active:${userId}`);
  
  let text = '📧 *Gmail Telegram Bot*\n━━━━━━━━━━━━━━━━━━━━\n\n';
  
  if (accounts.length > 0) {
    text += `👤 当前账户: ${active || '未选择'}\n📊 已绑定 ${accounts.length} 个账户\n\n`;
    text += '使用下方按钮操作 👇';
  } else {
    text += '👋 欢迎使用！\n\n请点击 *👤 账户管理* 添加 Gmail 账户';
  }

  await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'Markdown',
    reply_markup: getMainKeyboard()
  });
}

// ==================== 邮件列表 ====================
async function sendMailList(chatId, userId, query, pageToken, editMsgId, env) {
  const account = await getActiveAccount(userId, env);
  
  if (!account) {
    const method = editMsgId ? 'editMessageText' : 'sendMessage';
    const params = {
      chat_id: chatId,
      text: '⚠️ 请先绑定账户\n\n点击 *👤 账户管理* 添加 Gmail',
      parse_mode: 'Markdown'
    };
    if (editMsgId) params.message_id = editMsgId;
    else params.reply_markup = getMainKeyboard();
    await sendTelegram(env.BOT_TOKEN, method, params);
    return;
  }

  await env.USER_TOKENS.put(`lastquery:${userId}`, query, { expirationTtl: 3600 });

  const listUrl = new URL('https://gmail.googleapis.com/gmail/v1/users/me/messages');
  listUrl.searchParams.set('maxResults', PAGE_SIZE);
  listUrl.searchParams.set('q', query);
  if (pageToken) listUrl.searchParams.set('pageToken', pageToken);

  const listResp = await fetch(listUrl, {
    headers: { Authorization: `Bearer ${account.access_token}` }
  });
  const listData = await listResp.json();

  if (!listData.messages?.length) {
    const method = editMsgId ? 'editMessageText' : 'sendMessage';
    const params = {
      chat_id: chatId,
      text: `📭 ${query}\n\n没有找到匹配的邮件`,
      reply_markup: {
        inline_keyboard: [[{ text: '🔄 刷新', callback_data: `ref:${query.substring(0, 50)}` }]]
      }
    };
    if (editMsgId) params.message_id = editMsgId;
    await sendTelegram(env.BOT_TOKEN, method, params);
    return;
  }

  const mails = [];
  for (const msg of listData.messages) {
    const detailResp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=metadata&metadataHeaders=From&metadataHeaders=Subject&metadataHeaders=Date`,
      { headers: { Authorization: `Bearer ${account.access_token}` } }
    );
    const detail = await detailResp.json();
    const headers = detail.payload?.headers || [];
    const getHeader = (n) => headers.find(h => h.name.toLowerCase() === n.toLowerCase())?.value || '';
    
    let from = getHeader('From');
    const emailMatch = from.match(/[\w.-]+@[\w.-]+\.[a-z]+/i);
    if (emailMatch) {
      from = from.replace(emailMatch[0], '').replace(/[<>"]/g, '').trim() || emailMatch[0];
    }
    
    mails.push({
      id: msg.id,
      from: from.substring(0, 20),
      subject: (getHeader('Subject') || '(无主题)').substring(0, 30),
      date: formatDate(getHeader('Date')),
      unread: detail.labelIds?.includes('UNREAD'),
      starred: detail.labelIds?.includes('STARRED')
    });
  }

  await storeMailIds(userId, mails, env);

  let text = `📬 ${query.substring(0, 30)}\n━━━━━━━━━━━━━━━━━━━━\n\n`;
  mails.forEach((m, i) => {
    const icon = m.unread ? '🔵' : '⚪️';
    const star = m.starred ? '⭐' : '';
    text += `${icon}${star} ${i + 1}. ${m.subject}\n    📤 ${m.from} · ${m.date}\n\n`;
  });

  const buttons = [];
  
  for (let i = 0; i < mails.length; i += 3) {
    const row = [];
    for (let j = i; j < Math.min(i + 3, mails.length); j++) {
      const icon = mails[j].unread ? '🔵' : '📧';
      row.push({ text: `${icon} ${j + 1}`, callback_data: `m:${j}` });
    }
    buttons.push(row);
  }

  const navRow = [];
  navRow.push({ text: '🔄 刷新', callback_data: `ref:${query.substring(0, 50)}` });
  
  if (listData.nextPageToken) {
    const pageKey = `page:${userId}:${Date.now()}`;
    await env.USER_TOKENS.put(pageKey, JSON.stringify({ query, token: listData.nextPageToken }), { expirationTtl: 3600 });
    navRow.push({ text: '➡️ 下一页', callback_data: `pg:${Date.now()}` });
  }
  buttons.push(navRow);
  buttons.push([{ text: '✅ 全部已读', callback_data: 'readall' }]);

  const method = editMsgId ? 'editMessageText' : 'sendMessage';
  const params = {
    chat_id: chatId,
    text,
    reply_markup: { inline_keyboard: buttons }
  };
  if (editMsgId) params.message_id = editMsgId;
  
  await sendTelegram(env.BOT_TOKEN, method, params);
}

// ==================== HTML 转义函数 ====================
function escapeHtml(text) {
  if (!text) return '';
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ==================== 邮件详情 ====================
async function sendMailDetail(chatId, userId, mailId, editMsgId, showFull, env) {
  const account = await getActiveAccount(userId, env);
  
  if (!account) {
    await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
      chat_id: chatId,
      text: '⚠️ 登录已过期，请重新绑定账户',
      reply_markup: getMainKeyboard()
    });
    return;
  }

  const resp = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/messages/${mailId}?format=full`,
    { headers: { Authorization: `Bearer ${account.access_token}` } }
  );
  
  if (!resp.ok) {
    const method = editMsgId ? 'editMessageText' : 'sendMessage';
    await sendTelegram(env.BOT_TOKEN, method, {
      chat_id: chatId,
      message_id: editMsgId,
      text: '❌ 获取邮件失败',
      reply_markup: { inline_keyboard: [[{ text: '⬅️ 返回', callback_data: 'back' }]] }
    });
    return;
  }

  const mail = await resp.json();
  const headers = mail.payload?.headers || [];
  const getHeader = (n) => headers.find(h => h.name.toLowerCase() === n.toLowerCase())?.value || '';

  const from = getHeader('From');
  const subject = getHeader('Subject') || '(无主题)';
  const date = formatDate(getHeader('Date'));
  const unread = mail.labelIds?.includes('UNREAD');
  const starred = mail.labelIds?.includes('STARRED');

  let fromName = from;
  let fromEmail = from;
  const emailMatch = from.match(/[\w.-]+@[\w.-]+\.[a-z]+/i);
  if (emailMatch) {
    fromEmail = emailMatch[0];
    fromName = from.replace(/[<>]/g, '').replace(emailMatch[0], '').replace(/"/g, '').trim() || fromEmail;
  }

  const maxLen = showFull ? MAX_CONTENT_LENGTH : PREVIEW_LENGTH;
  const content = extractContent(mail.payload, maxLen, showFull);
  const attachments = getAttachments(mail.payload);

  // 使用 HTML 格式构建消息
  let text = `${unread ? '🔵 未读' : '⚪️ 已读'}${starred ? ' ⭐' : ''}\n`;
  text += '━━━━━━━━━━━━━━━━━━━━\n';
  text += `📋 <b>${escapeHtml(subject)}</b>\n\n`;
  text += `👤 ${escapeHtml(fromName)}\n`;
  text += `📧 ${escapeHtml(fromEmail)}\n`;
  text += `🕐 ${escapeHtml(date)}\n`;
  if (attachments.length) text += `📎 附件: ${attachments.length} 个\n`;
  text += '━━━━━━━━━━━━━━━━━━━━\n\n';
  text += content; // content 已经是 HTML 格式

  await env.USER_TOKENS.put(`current:${userId}`, mailId, { expirationTtl: 3600 });

  const buttons = [];
  
  buttons.push([
    { text: unread ? '✅ 已读' : '📩 未读', callback_data: unread ? 'do:read' : 'do:unread' },
    { text: starred ? '⭐ 取消' : '⭐ 星标', callback_data: starred ? 'do:unstar' : 'do:star' },
    { text: '🗑️', callback_data: 'do:delete' }
  ]);

  if (!showFull && content.includes('(点击查看完整)')) {
    buttons.push([{ text: '📖 查看完整内容', callback_data: 'do:full' }]);
  }
  
  // 网页预览按钮
  const viewLink = await generateViewLink(userId, mailId, account.email, env);
  buttons.push([{ text: '🌐 在浏览器中查看', url: viewLink }]);

  const searchEmail = fromEmail.substring(0, 25);
  buttons.push([{ text: `🔍 搜索 ${fromName.substring(0, 10)} 的邮件`, callback_data: `sf:${searchEmail}` }]);

  if (attachments.length > 0) {
    const attRow = [];
    attachments.slice(0, 3).forEach((att, i) => {
      attRow.push({ text: `📎 ${att.name.substring(0, 10)}`, callback_data: `att:${i}` });
    });
    buttons.push(attRow);
  }

  buttons.push([{ text: '⬅️ 返回列表', callback_data: 'back' }]);

  const method = editMsgId ? 'editMessageText' : 'sendMessage';
  const params = {
    chat_id: chatId,
    text,
    parse_mode: 'HTML', // 使用 HTML 解析模式
    reply_markup: { inline_keyboard: buttons }
  };
  if (editMsgId) params.message_id = editMsgId;
  
  await sendTelegram(env.BOT_TOKEN, method, params);
}

// ==================== 内容提取 ====================
function extractContent(payload, maxLen, isFullMode) {
  let bodyData = findBody(payload, 'text/html') || findBody(payload, 'text/plain');
  
  if (!bodyData) return '(无内容)';

  try {
    const base64 = bodyData.replace(/-/g, '+').replace(/_/g, '/');
    let text = decodeBase64(base64);

    // 清理 HTML 标签，保留文本内容和链接
    text = text.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '');
    text = text.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '');
    
    // 处理图片标签 - 转换为 [图片] 链接（带换行）
    text = text.replace(/<img[^>]*src=["']([^"']+)["'][^>]*alt=["']([^"']*)["'][^>]*>/gi, (match, src, alt) => {
      const linkText = alt || '图片';
      return `\n<a href="${src}">[${linkText}]</a>\n`;
    });
    text = text.replace(/<img[^>]*src=["']([^"']+)["'][^>]*>/gi, (match, src) => {
      return `\n<a href="${src}">[图片]</a>\n`;
    });
    
    // 处理视频标签
    text = text.replace(/<video[^>]*src=["']([^"']+)["'][^>]*>/gi, (match, src) => {
      return `\n<a href="${src}">[视频]</a>\n`;
    });
    text = text.replace(/<source[^>]*src=["']([^"']+)["'][^>]*>/gi, (match, src) => {
      if (src.match(/\.(mp4|webm|ogg|mov)$/i)) {
        return `\n<a href="${src}">[视频]</a>\n`;
      }
      return '';
    });
    
    // 提取链接并转换为 Telegram HTML 格式
    const links = [];
    text = text.replace(/<a\s+[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi, (match, url, linkText) => {
      // 清理链接文本中的HTML标签
      let cleanText = linkText.replace(/<[^>]+>/g, '').trim();
      
      // 如果链接文本为空，尝试生成友好的文本
      if (!cleanText) {
        if (url.match(/\.(jpg|jpeg|png|gif|webp|svg|bmp)(\?|$)/i)) {
          cleanText = '[图片]';
        } else if (url.match(/\.(mp4|webm|ogg|mov|avi|flv)(\?|$)/i)) {
          cleanText = '[视频]';
        } else if (url.match(/\.(mp3|wav|ogg|m4a|flac)(\?|$)/i)) {
          cleanText = '[音频]';
        } else if (url.match(/\.(pdf|doc|docx|xls|xlsx|ppt|pptx)(\?|$)/i)) {
          cleanText = '[文档]';
        } else if (url.length > 50) {
          try {
            const urlObj = new URL(url);
            cleanText = `[${urlObj.hostname}]`;
          } catch {
            cleanText = '[链接]';
          }
        } else {
          cleanText = '[链接]';
        }
      } else if (cleanText === url && url.length > 50) {
        if (url.match(/\.(jpg|jpeg|png|gif|webp|svg|bmp)(\?|$)/i)) {
          cleanText = '[图片]';
        } else if (url.match(/\.(mp4|webm|ogg|mov)(\?|$)/i)) {
          cleanText = '[视频]';
        } else {
          try {
            const urlObj = new URL(url);
            cleanText = `[${urlObj.hostname}]`;
          } catch {
            cleanText = '[链接]';
          }
        }
      }
      
      // 清理特殊字符
      cleanText = cleanText
        .replace(/&nbsp;/gi, ' ')
        .replace(/&amp;/gi, '&')
        .replace(/&lt;/gi, '<')
        .replace(/&gt;/gi, '>')
        .replace(/&quot;/gi, '"');
      
      // 清理URL中的HTML实体
      url = url
        .replace(/&amp;/gi, '&')
        .replace(/&lt;/gi, '<')
        .replace(/&gt;/gi, '>')
        .replace(/&quot;/gi, '"');
      
      const placeholder = `__LINK_${links.length}__`;
      links.push({ url, text: cleanText });
      return ` ${placeholder} `;  // 链接前后加空格，避免粘连
    });
    
    // 清理HTML标签，保留换行
    text = text.replace(/<br\s*\/?>/gi, '\n');
    text = text.replace(/<\/p>/gi, '\n\n');
    text = text.replace(/<p[^>]*>/gi, '');
    text = text.replace(/<\/div>/gi, '\n');
    text = text.replace(/<div[^>]*>/gi, '');
    text = text.replace(/<\/li>/gi, '\n');
    text = text.replace(/<li[^>]*>/gi, '\n• ');
    text = text.replace(/<ul[^>]*>/gi, '\n');
    text = text.replace(/<\/ul>/gi, '\n');
    text = text.replace(/<ol[^>]*>/gi, '\n');
    text = text.replace(/<\/ol>/gi, '\n');
    text = text.replace(/<\/tr>/gi, '\n');
    text = text.replace(/<tr[^>]*>/gi, '');
    text = text.replace(/<\/(h1|h2|h3|h4|h5|h6)>/gi, '\n\n');
    text = text.replace(/<(h1|h2|h3|h4|h5|h6)[^>]*>/gi, '\n');
    text = text.replace(/<\/?(table|tbody|thead|td|th|span|strong|b|i|em|u)[^>]*>/gi, '');
    // 移除所有其他HTML标签
    text = text.replace(/<[^>]+>/g, '');
    
    // 清理所有HTML实体（完整列表）
    const htmlEntities = {
      '&nbsp;': ' ',
      '&amp;': '&',
      '&lt;': '<',
      '&gt;': '>',
      '&quot;': '"',
      '&apos;': "'",
      '&copy;': '\u00A9',
      '&reg;': '\u00AE',
      '&trade;': '\u2122',
      '&euro;': '\u20AC',
      '&pound;': '\u00A3',
      '&yen;': '\u00A5',
      '&cent;': '\u00A2',
      '&sect;': '\u00A7',
      '&deg;': '\u00B0',
      '&plusmn;': '\u00B1',
      '&para;': '\u00B6',
      '&middot;': '\u00B7',
      '&ndash;': '\u2013',
      '&mdash;': '\u2014',
      '&lsquo;': '\u2018',
      '&rsquo;': '\u2019',
      '&ldquo;': '\u201C',
      '&rdquo;': '\u201D',
      '&bull;': '\u2022',
      '&hellip;': '\u2026'
    };
    
    // 替换命名实体
    for (const [entity, char] of Object.entries(htmlEntities)) {
      text = text.replace(new RegExp(entity, 'gi'), char);
    }
    
    // 清理数字实体
    text = text
      .replace(/&#8202;/g, '')  // 细空格
      .replace(/&#8203;/g, '')  // 零宽空格
      .replace(/&#160;/g, ' ')  // 不间断空格
      .replace(/&#32;/g, ' ')   // 普通空格
      .replace(/&#96;/g, '')    // 反引号
      .replace(/&#x60;/g, '')   // 十六进制的96
      .replace(/&#x2018;/g, '\u2018') // 左单引号
      .replace(/&#x2019;/g, '\u2019') // 右单引号
      .replace(/&#x201C;/g, '\u201C') // 左双引号
      .replace(/&#x201D;/g, '\u201D') // 右双引号
      .replace(/&#169;/g, '\u00A9')   // 版权符号
      .replace(/&#xa9;/g, '\u00A9')   // 版权符号（十六进制）
      .replace(/&#\d+;/g, '')    // 移除其他数字实体
      .replace(/&#x[0-9a-fA-F]+;/g, ''); // 移除十六进制实体
    
    // 移除Unicode控制字符和零宽字符（保留换行符\n）
    text = text
      .replace(/[\u200B-\u200D\uFEFF]/g, '')  // 零宽字符
      .replace(/[\u0000-\u0008\u000B-\u000C\u000E-\u001F\u007F-\u009F]/g, '');  // 控制字符（保留\n=\u000A和\r=\u000D）
    
    // 统一换行符
    text = text.replace(/\r\n/g, '\n');
    text = text.replace(/\r/g, '\n');
    
    // 清理空格和换行
    text = text.replace(/[ \t]+/g, ' ');  // 多个空格合并为一个
    text = text.replace(/ *\n */g, '\n'); // 移除换行符前后的空格
    text = text.replace(/\n{3,}/g, '\n\n'); // 最多保留两个连续换行
    text = text.replace(/^\n+/, '');  // 移除开头的换行
    text = text.replace(/\n+$/, '');  // 移除结尾的换行
    text = text.trim();
    
    // 清理多余的项目符号
    text = text.replace(/\n•\s*\n/g, '\n'); // 移除空的列表项
    text = text.replace(/•\s+•/g, '•'); // 合并连续的项目符号
    
    // 移除开头的孤立数字
    text = text.replace(/^\d+[\s\n]+/, '');
    text = text.replace(/\b\d+\s*(?=__LINK_)/g, '');

    // 先转义普通文本
    text = escapeHtml(text);

    // 恢复链接
    links.forEach((link, i) => {
      const placeholder = escapeHtml(`__LINK_${i}__`);
      const escapedText = escapeHtml(link.text);
      const escapedUrl = link.url.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
      const linkHtml = `<a href="${escapedUrl}">${escapedText}</a>`;
      text = text.replace(placeholder, linkHtml);
    });
    
    // 最后清理
    text = text.replace(/^\d+\s*/g, '');
    text = text.trim();

    // 截断处理
    if (text.length > maxLen) {
      let cutPos = maxLen;
      const linkPattern = /<a href="[^"]*">[^<]*<\/a>/g;
      let match;
      let lastSafePos = 0;
      
      while ((match = linkPattern.exec(text)) !== null) {
        if (match.index < maxLen) {
          lastSafePos = match.index + match[0].length;
        } else {
          break;
        }
      }
      
      if (lastSafePos > maxLen - 100) {
        cutPos = lastSafePos;
      }
      
      text = text.substring(0, cutPos);
      
      const openTags = (text.match(/<a href/g) || []).length;
      const closeTags = (text.match(/<\/a>/g) || []).length;
      if (openTags > closeTags) {
        text = text.replace(/<a href="[^"]*">(?!.*<\/a>)[^<]*$/g, '');
      }
      
      if (isFullMode) {
        text += '\n\n... (内容过长，点击浏览器查看完整)';
      } else {
        text += '\n\n... (点击查看完整)';
      }
    }

    return text || '(无内容)';
  } catch (e) {
    console.error('Content extraction error:', e);
    try {
      const plainData = findBody(payload, 'text/plain');
      if (plainData) {
        const base64 = plainData.replace(/-/g, '+').replace(/_/g, '/');
        let plainText = decodeBase64(base64);
        plainText = plainText
          .replace(/&#\d+;/g, '')
          .replace(/&#x[0-9a-fA-F]+;/g, '')
          .replace(/&nbsp;/gi, ' ')
          .replace(/&amp;/gi, '&')
          .replace(/&copy;/gi, '©')
          .replace(/[\u200B-\u200D\uFEFF]/g, '')
          .trim();
        
        plainText = plainText.replace(/^\d+[\s\n]+/, '');
        
        if (plainText.length > maxLen) {
          plainText = plainText.substring(0, maxLen) + '\n\n... (点击查看完整)';
        }
        return escapeHtml(plainText);
      }
    } catch (e2) {
      console.error('Plain text extraction error:', e2);
    }
    return '(解析失败)';
  }
}

function findBody(part, mimeType) {
  if (part.mimeType === mimeType && part.body?.data) {
    return part.body.data;
  }
  if (part.parts) {
    for (const p of part.parts) {
      const found = findBody(p, mimeType);
      if (found) return found;
    }
  }
  return null;
}

function getAttachments(payload) {
  const atts = [];
  function scan(part) {
    if (part.filename && part.body?.attachmentId) {
      atts.push({
        name: part.filename,
        id: part.body.attachmentId,
        size: part.body.size || 0
      });
    }
    if (part.parts) part.parts.forEach(scan);
  }
  scan(payload);
  return atts;
}

// ==================== 统计 ====================
async function sendStats(chatId, userId, editMsgId, env) {
  const account = await getActiveAccount(userId, env);
  
  if (!account) {
    await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
      chat_id: chatId,
      text: '⚠️ 请先绑定账户',
      reply_markup: getMainKeyboard()
    });
    return;
  }

  const token = account.access_token;
  const todayTs = getTodayTimestamp();

  const [profileResp, unreadResp, todayResp, starredResp] = await Promise.all([
    fetch('https://gmail.googleapis.com/gmail/v1/users/me/profile', { headers: { Authorization: `Bearer ${token}` } }),
    fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages?q=is:unread&maxResults=1', { headers: { Authorization: `Bearer ${token}` } }),
    fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages?q=after:${todayTs}&maxResults=1`, { headers: { Authorization: `Bearer ${token}` } }),
    fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages?q=is:starred&maxResults=1', { headers: { Authorization: `Bearer ${token}` } })
  ]);

  const profile = await profileResp.json();
  const unread = await unreadResp.json();
  const today = await todayResp.json();
  const starred = await starredResp.json();

  let text = '📊 *邮箱统计*\n━━━━━━━━━━━━━━━━━━━━\n\n';
  text += `📧 ${profile.emailAddress}\n\n`;
  text += `📬 未读: *${unread.resultSizeEstimate || 0}*\n`;
  text += `📅 今日: *${today.resultSizeEstimate || 0}*\n`;
  text += `⭐ 星标: *${starred.resultSizeEstimate || 0}*\n`;
  text += `📁 总数: *${profile.messagesTotal || 0}*\n`;

  const buttons = [
    [
      { text: '📬 查看未读', callback_data: 'list:is:unread' },
      { text: '📅 查看今日', callback_data: `list:after:${todayTs}` }
    ],
    [{ text: '🔄 刷新', callback_data: 'stats:refresh' }]
  ];

  const method = editMsgId ? 'editMessageText' : 'sendMessage';
  const params = {
    chat_id: chatId,
    text,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: buttons }
  };
  if (editMsgId) params.message_id = editMsgId;
  
  await sendTelegram(env.BOT_TOKEN, method, params);
}

// ==================== 账户管理 ====================
async function sendAccountManager(chatId, userId, editMsgId, env) {
  const accounts = await getAccountList(userId, env);
  const active = await env.USER_TOKENS.get(`active:${userId}`);

  let text = '👤 *账户管理*\n━━━━━━━━━━━━━━━━━━━━\n\n';
  
  if (accounts.length === 0) {
    text += '📭 尚未绑定任何账户\n\n点击下方按钮添加';
  } else {
    text += `已绑定 *${accounts.length}* 个账户:\n\n`;
    accounts.forEach((email, i) => {
      const isActive = email === active;
      text += `${isActive ? '✅' : '⚪️'} ${i + 1}. ${email}${isActive ? ' (当前)' : ''}\n`;
    });
  }

  const buttons = [];
  
  accounts.forEach((email, i) => {
    if (i % 2 === 0) buttons.push([]);
    const row = buttons[buttons.length - 1];
    const isActive = email === active;
    row.push({
      text: `${isActive ? '✅' : '📧'} ${email.substring(0, 15)}`,
      callback_data: `sw:${i}`
    });
  });

  await env.USER_TOKENS.put(`accmap:${userId}`, JSON.stringify(accounts), { expirationTtl: 3600 });

  buttons.push([
    { text: '➕ 添加账户', callback_data: 'add' },
    { text: '🗑️ 删除账户', callback_data: 'delmenu' }
  ]);
  buttons.push([{ text: '🔄 刷新', callback_data: 'acc:refresh' }]);

  const method = editMsgId ? 'editMessageText' : 'sendMessage';
  const params = {
    chat_id: chatId,
    text,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: buttons }
  };
  if (editMsgId) params.message_id = editMsgId;
  
  await sendTelegram(env.BOT_TOKEN, method, params);
}

// ==================== 添加账户 ====================
async function sendLoginLink(chatId, userId, env) {
  const origin = await env.USER_TOKENS.get('origin');
  const nonce = crypto.randomUUID();
  await env.USER_TOKENS.put(`nonce:${userId}`, nonce, { expirationTtl: 600 });

  const state = btoa(JSON.stringify({ userId, nonce }));
  const authUrl = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  authUrl.searchParams.set('client_id', env.GOOGLE_CLIENT_ID);
  authUrl.searchParams.set('redirect_uri', `${origin}/oauth/callback`);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('scope', GMAIL_SCOPES);
  authUrl.searchParams.set('access_type', 'offline');
  authUrl.searchParams.set('prompt', 'consent');
  authUrl.searchParams.set('state', state);

  await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
    chat_id: chatId,
    text: '🔐 *添加 Gmail 账户*\n\n请点击下方按钮授权',
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [
        [{ text: '🔐 授权 Gmail', url: authUrl.toString() }],
        [{ text: '⬅️ 返回', callback_data: 'acc:refresh' }]
      ]
    }
  });
}

// ==================== 设置 ====================
async function sendSettings(chatId, userId, env) {
  const active = await env.USER_TOKENS.get(`active:${userId}`);
  let pushEnabled = false;
  
  if (active) {
    const pushData = await env.USER_TOKENS.get(`push:${userId}:${active}`);
    pushEnabled = pushData ? JSON.parse(pushData).enabled : false;
  }

  let text = '⚙️ *设置*\n━━━━━━━━━━━━━━━━━━━━\n\n';
  text += `👤 账户: ${active || '未绑定'}\n`;
  text += `🔔 推送: ${pushEnabled ? '已开启' : '已关闭'}\n`;

  const buttons = [
    [{ text: '👤 账户管理', callback_data: 'acc:refresh' }]
  ];

  if (env.PUBSUB_TOPIC && active) {
    buttons.push([{
      text: pushEnabled ? '🔕 关闭推送' : '🔔 开启推送',
      callback_data: pushEnabled ? 'push:off' : 'push:on'
    }]);
  }

  buttons.push([{ text: '🔍 搜索帮助', callback_data: 'help' }]);

  await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: buttons }
  });
}

// ==================== 搜索帮助 ====================
async function sendSearchHelp(chatId, env) {
  const text = '🔍 *搜索邮件*\n\n发送: 搜索 关键词\n\n*示例:*\n• 搜索 会议\n• 搜索 from:test@qq.com\n• 搜索 subject:周报\n• 搜索 has:attachment';

  const buttons = [
    [
      { text: '📬 未读', callback_data: 'list:is:unread' },
      { text: '⭐ 星标', callback_data: 'list:is:starred' },
      { text: '📎 附件', callback_data: 'list:has:attachment' }
    ],
    [
      { text: '📅 本周', callback_data: 'list:newer_than:7d' },
      { text: '📆 本月', callback_data: 'list:newer_than:30d' }
    ]
  ];

  await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: buttons }
  });
}

// ==================== 批量已读 ====================
async function markAllRead(chatId, userId, env) {
  const account = await getActiveAccount(userId, env);
  
  if (!account) {
    await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
      chat_id: chatId,
      text: '⚠️ 请先绑定账户',
      reply_markup: getMainKeyboard()
    });
    return;
  }

  const listResp = await fetch(
    'https://gmail.googleapis.com/gmail/v1/users/me/messages?q=is:unread&maxResults=100',
    { headers: { Authorization: `Bearer ${account.access_token}` } }
  );
  const listData = await listResp.json();

  if (!listData.messages?.length) {
    await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
      chat_id: chatId,
      text: '✅ 没有未读邮件',
      reply_markup: getMainKeyboard()
    });
    return;
  }

  await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/batchModify', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${account.access_token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      ids: listData.messages.map(m => m.id),
      removeLabelIds: ['UNREAD']
    })
  });

  await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
    chat_id: chatId,
    text: `✅ 已将 *${listData.messages.length}* 封邮件标记为已读`,
    parse_mode: 'Markdown',
    reply_markup: getMainKeyboard()
  });
}

// ==================== 回调处理 ====================
async function handleCallback(query, env) {
  const chatId = query.message.chat.id;
  const userId = String(query.from.id);
  const msgId = query.message.message_id;
  const data = query.data;

  await sendTelegram(env.BOT_TOKEN, 'answerCallbackQuery', {
    callback_query_id: query.id
  });

  if (data === 'stats:refresh') {
    await sendStats(chatId, userId, msgId, env);
    return;
  }

  if (data === 'acc:refresh') {
    await sendAccountManager(chatId, userId, msgId, env);
    return;
  }

  if (data === 'add') {
    await sendLoginLink(chatId, userId, env);
    return;
  }

  if (data.startsWith('sw:')) {
    const index = parseInt(data.substring(3));
    const accRaw = await env.USER_TOKENS.get(`accmap:${userId}`);
    if (accRaw) {
      const accounts = JSON.parse(accRaw);
      if (accounts[index]) {
        await env.USER_TOKENS.put(`active:${userId}`, accounts[index]);
      }
    }
    await sendAccountManager(chatId, userId, msgId, env);
    return;
  }

  if (data === 'delmenu') {
    const accounts = await getAccountList(userId, env);
    const buttons = accounts.map((email, i) => ([{
      text: `🗑️ ${email}`,
      callback_data: `del:${i}`
    }]));
    buttons.push([{ text: '⬅️ 返回', callback_data: 'acc:refresh' }]);

    await sendTelegram(env.BOT_TOKEN, 'editMessageText', {
      chat_id: chatId,
      message_id: msgId,
      text: '🗑️ 选择要删除的账户:',
      reply_markup: { inline_keyboard: buttons }
    });
    return;
  }

  if (data.startsWith('del:')) {
    const index = parseInt(data.substring(4));
    const accounts = await getAccountList(userId, env);
    const email = accounts[index];
    
    if (email) {
      await env.USER_TOKENS.delete(`token:${userId}:${email}`);
      await env.USER_TOKENS.delete(`push:${userId}:${email}`);
      accounts.splice(index, 1);
      await env.USER_TOKENS.put(`accounts:${userId}`, JSON.stringify(accounts));
      
      const active = await env.USER_TOKENS.get(`active:${userId}`);
      if (active === email && accounts.length > 0) {
        await env.USER_TOKENS.put(`active:${userId}`, accounts[0]);
      } else if (accounts.length === 0) {
        await env.USER_TOKENS.delete(`active:${userId}`);
      }
    }
    
    await sendAccountManager(chatId, userId, msgId, env);
    return;
  }

  if (data === 'help') {
    await sendSearchHelp(chatId, env);
    return;
  }

  if (data.startsWith('list:')) {
    const query = data.substring(5);
    await sendMailList(chatId, userId, query, null, msgId, env);
    return;
  }

  if (data.startsWith('ref:')) {
    const query = data.substring(4);
    await sendMailList(chatId, userId, query, null, msgId, env);
    return;
  }

  if (data.startsWith('pg:')) {
    const ts = data.substring(3);
    const pageKey = `page:${userId}:${ts}`;
    const pageData = await env.USER_TOKENS.get(pageKey);
    if (pageData) {
      const { query, token } = JSON.parse(pageData);
      await sendMailList(chatId, userId, query, token, msgId, env);
    }
    return;
  }

  if (data.startsWith('m:')) {
    const index = parseInt(data.substring(2));
    const mailId = await getMailId(userId, index, env);
    if (mailId) {
      await sendMailDetail(chatId, userId, mailId, msgId, false, env);
    }
    return;
  }

  if (data.startsWith('do:')) {
    const action = data.substring(3);
    const mailId = await env.USER_TOKENS.get(`current:${userId}`);
    
    if (!mailId) return;

    const account = await getActiveAccount(userId, env);
    if (!account) return;

    if (action === 'full') {
      await sendMailDetail(chatId, userId, mailId, msgId, true, env);
      return;
    }

    const actions = {
      read: { removeLabelIds: ['UNREAD'] },
      unread: { addLabelIds: ['UNREAD'] },
      star: { addLabelIds: ['STARRED'] },
      unstar: { removeLabelIds: ['STARRED'] }
    };

    if (actions[action]) {
      await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${mailId}/modify`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${account.access_token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(actions[action])
      });
      await sendMailDetail(chatId, userId, mailId, msgId, false, env);
      return;
    }

    if (action === 'delete') {
      await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${mailId}/trash`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${account.access_token}` }
      });
      await sendTelegram(env.BOT_TOKEN, 'editMessageText', {
        chat_id: chatId,
        message_id: msgId,
        text: '🗑️ 已移至垃圾箱',
        reply_markup: { inline_keyboard: [[{ text: '⬅️ 返回列表', callback_data: 'back' }]] }
      });
      return;
    }
  }

  if (data.startsWith('sf:')) {
    const email = data.substring(3);
    await sendMailList(chatId, userId, `from:${email}`, null, msgId, env);
    return;
  }

  if (data.startsWith('att:')) {
    const index = parseInt(data.substring(4));
    const mailId = await env.USER_TOKENS.get(`current:${userId}`);
    const account = await getActiveAccount(userId, env);
    
    if (!mailId || !account) return;

    const mailResp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${mailId}?format=full`,
      { headers: { Authorization: `Bearer ${account.access_token}` } }
    );
    const mail = await mailResp.json();
    const attachments = getAttachments(mail.payload);
    const att = attachments[index];

    if (!att) return;

    const attResp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${mailId}/attachments/${att.id}`,
      { headers: { Authorization: `Bearer ${account.access_token}` } }
    );
    const attData = await attResp.json();

    const base64 = attData.data.replace(/-/g, '+').replace(/_/g, '/');
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);

    const formData = new FormData();
    formData.append('chat_id', chatId);
    formData.append('document', new Blob([bytes]), att.name);

    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: formData
    });
    return;
  }

  if (data === 'back') {
    const lastQuery = await env.USER_TOKENS.get(`lastquery:${userId}`) || 'in:inbox';
    await sendMailList(chatId, userId, lastQuery, null, msgId, env);
    return;
  }

  if (data === 'readall') {
    await markAllRead(chatId, userId, env);
    return;
  }

  if (data === 'push:on' || data === 'push:off') {
    const enable = data === 'push:on';
    const active = await env.USER_TOKENS.get(`active:${userId}`);
    
    if (!active) return;

    if (enable && env.PUBSUB_TOPIC) {
      const account = await getActiveAccount(userId, env);
      if (account) {
        const watchResp = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/watch', {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${account.access_token}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            topicName: env.PUBSUB_TOPIC,
            labelIds: ['INBOX']
          })
        });
        const watchData = await watchResp.json();
        
        await env.USER_TOKENS.put(`push:${userId}:${active}`, JSON.stringify({
          enabled: true,
          historyId: watchData.historyId,
          expiry: watchData.expiration
        }));
      }
    } else {
      await env.USER_TOKENS.put(`push:${userId}:${active}`, JSON.stringify({ enabled: false }));
    }

    await sendSettings(chatId, userId, env);
  }
}

// ==================== Pub/Sub 推送 ====================
async function handlePubSubPush(message, env) {
  if (!message.message?.data) return;

  const data = JSON.parse(atob(message.message.data));
  const { emailAddress, historyId } = data;

  const list = await env.USER_TOKENS.list({ prefix: 'push:' });
  
  for (const key of list.keys) {
    const pushData = JSON.parse(await env.USER_TOKENS.get(key.name) || '{}');
    if (!pushData.enabled) continue;

    const parts = key.name.split(':');
    const usrId = parts[1];
    const email = parts.slice(2).join(':');
    
    if (email !== emailAddress) continue;

    const tokenRaw = await env.USER_TOKENS.get(`token:${usrId}:${email}`);
    if (!tokenRaw) continue;

    let token = JSON.parse(tokenRaw);
    
    if (Date.now() > token.expiry - 60000) {
      const refreshed = await refreshToken(token.refresh_token, env);
      if (refreshed) {
        token.access_token = refreshed.access_token;
        token.expiry = Date.now() + refreshed.expires_in * 1000;
        await env.USER_TOKENS.put(`token:${usrId}:${email}`, JSON.stringify(token));
      } else {
        continue;
      }
    }

    const historyResp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/history?startHistoryId=${pushData.historyId || historyId}&historyTypes=messageAdded&labelId=INBOX`,
      { headers: { Authorization: `Bearer ${token.access_token}` } }
    );
    const historyData = await historyResp.json();

    if (historyData.history) {
      for (const h of historyData.history) {
        if (h.messagesAdded) {
          for (const m of h.messagesAdded) {
            const mailResp = await fetch(
              `https://gmail.googleapis.com/gmail/v1/users/me/messages/${m.message.id}?format=metadata&metadataHeaders=From&metadataHeaders=Subject`,
              { headers: { Authorization: `Bearer ${token.access_token}` } }
            );
            const mail = await mailResp.json();
            const headers = mail.payload?.headers || [];
            const from = headers.find(h => h.name === 'From')?.value || '';
            const subject = headers.find(h => h.name === 'Subject')?.value || '(无主题)';

            let fromName = from;
            const emailMatch = from.match(/[\w.-]+@[\w.-]+\.[a-z]+/i);
            if (emailMatch) {
              fromName = from.replace(emailMatch[0], '').replace(/[<>"]/g, '').trim() || emailMatch[0];
            }

            await env.USER_TOKENS.put(`active:${usrId}`, email);
            await env.USER_TOKENS.put(`current:${usrId}`, m.message.id, { expirationTtl: 3600 });

            const viewLink = await generateViewLink(usrId, m.message.id, email, env);

            await sendTelegram(env.BOT_TOKEN, 'sendMessage', {
              chat_id: usrId,
              text: `🔔 <b>新邮件</b>\n━━━━━━━━━━━━━━━━\n\n📧 ${escapeHtml(email)}\n👤 ${escapeHtml(fromName)}\n📋 ${escapeHtml(subject)}`,
              parse_mode: 'HTML',
              reply_markup: {
                inline_keyboard: [
                  [{ text: '🌐 在浏览器中查看', url: viewLink }],
                  [{ text: '📖 在 Telegram 查看', callback_data: 'do:full' }],
                  [
                    { text: '✅ 已读', callback_data: 'do:read' },
                    { text: '🗑️ 删除', callback_data: 'do:delete' }
                  ]
                ]
              }
            });
          }
        }
      }
    }

    pushData.historyId = historyData.historyId || historyId;
    await env.USER_TOKENS.put(key.name, JSON.stringify(pushData));
  }
}

// ==================== 续期 Watch ====================
async function renewAllWatches(env) {
  if (!env.PUBSUB_TOPIC) return;

  const list = await env.USER_TOKENS.list({ prefix: 'push:' });
  
  for (const key of list.keys) {
    const pushData = JSON.parse(await env.USER_TOKENS.get(key.name) || '{}');
    if (!pushData.enabled) continue;

    const parts = key.name.split(':');
    const usrId = parts[1];
    const email = parts.slice(2).join(':');

    const tokenRaw = await env.USER_TOKENS.get(`token:${usrId}:${email}`);
    if (!tokenRaw) continue;

    let token = JSON.parse(tokenRaw);
    
    if (Date.now() > token.expiry - 60000) {
      const refreshed = await refreshToken(token.refresh_token, env);
      if (refreshed) {
        token.access_token = refreshed.access_token;
        token.expiry = Date.now() + refreshed.expires_in * 1000;
        await env.USER_TOKENS.put(`token:${usrId}:${email}`, JSON.stringify(token));
      } else {
        continue;
      }
    }

    const watchResp = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/watch', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token.access_token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        topicName: env.PUBSUB_TOPIC,
        labelIds: ['INBOX']
      })
    });
    const watchData = await watchResp.json();

    if (watchData.historyId) {
      pushData.historyId = watchData.historyId;
      pushData.expiry = watchData.expiration;
      await env.USER_TOKENS.put(key.name, JSON.stringify(pushData));
    }
  }
}

// ==================== 解决方案 ====================

function getHomePage() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  
  <!-- 🔴 关键1：title标签中ONLY使用应用名称 -->
  <title>星霜的邮件助手</title>
  
  <!-- 🔴 关键2：Meta标签明确应用名称 -->
  <meta name="application-name" content="星霜的邮件助手">
  <meta name="description" content="通过 Telegram 安全便捷地访问和管理您的邮箱">
  
  <!-- 隐私政策链接标记 -->
  <link rel="privacy-policy" href="/privacy">
  <meta name="privacy-policy" content="https://emailbot.loushi.de5.net/privacy">
  
  <style>
    /* 样式代码保持不变 */
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }
    
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      color: #fff;
    }
    
    .top-nav {
      background: rgba(0, 0, 0, 0.2);
      backdrop-filter: blur(10px);
      padding: 15px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.1);
      position: sticky;
      top: 0;
      z-index: 100;
    }
    
    .top-nav-container {
      max-width: 1200px;
      margin: 0 auto;
      padding: 0 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    
    /* 🔴 关键3：nav-brand 也必须只使用应用名称 */
    .nav-brand {
      font-size: 20px;
      font-weight: 700;
      color: white;
      text-decoration: none;
    }
    
    .nav-links {
      display: flex;
      gap: 25px;
      align-items: center;
    }
    
    .nav-link {
      color: white;
      text-decoration: none;
      font-weight: 500;
      font-size: 15px;
      transition: all 0.3s ease;
      padding: 8px 15px;
      border-radius: 6px;
    }
    
    .nav-link:hover {
      background: rgba(255, 255, 255, 0.15);
    }
    
    .nav-link-privacy {
      background: rgba(255, 255, 255, 0.2);
      font-weight: 600;
    }
    
    .container {
      max-width: 1000px;
      margin: 0 auto;
      padding: 40px 20px;
    }
    
    .header {
      text-align: center;
      margin-bottom: 60px;
    }
    
    .logo {
      font-size: 80px;
      margin-bottom: 20px;
    }
    
    /* 🔴 关键4：H1 标签样式 */
    .title {
      font-size: 48px;
      font-weight: 700;
      margin-bottom: 30px;
      text-shadow: 0 2px 10px rgba(0,0,0,0.2);
    }
    
    /* 分离的描述文字样式 */
    .description {
      font-size: 20px;
      opacity: 0.9;
      margin-bottom: 30px;
    }
    
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: rgba(255,255,255,0.2);
      backdrop-filter: blur(10px);
      padding: 10px 24px;
      border-radius: 25px;
      font-weight: 600;
      font-size: 16px;
    }
    
    .status-dot {
      width: 8px;
      height: 8px;
      background: #10b981;
      border-radius: 50%;
      animation: pulse 2s infinite;
    }
    
    .content {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 30px;
      margin-bottom: 60px;
    }
    
    .card {
      background: rgba(255,255,255,0.15);
      backdrop-filter: blur(10px);
      border-radius: 20px;
      padding: 30px;
      border: 1px solid rgba(255,255,255,0.2);
      transition: all 0.3s ease;
    }
    
    .card:hover {
      transform: translateY(-5px);
      box-shadow: 0 10px 30px rgba(0,0,0,0.3);
      background: rgba(255,255,255,0.2);
    }
    
    .card-icon {
      font-size: 40px;
      margin-bottom: 15px;
    }
    
    .card h2 {
      font-size: 24px;
      margin-bottom: 10px;
    }
    
    .card p {
      opacity: 0.9;
      line-height: 1.6;
      font-size: 15px;
    }
    
    .features {
      background: rgba(255,255,255,0.1);
      backdrop-filter: blur(10px);
      border-radius: 20px;
      padding: 40px;
      margin-bottom: 40px;
      border: 1px solid rgba(255,255,255,0.2);
    }
    
    .features h2 {
      font-size: 32px;
      margin-bottom: 30px;
      text-align: center;
    }
    
    .feature-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 20px;
    }
    
    .feature-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 15px;
      background: rgba(255,255,255,0.1);
      border-radius: 12px;
      transition: all 0.3s ease;
    }
    
    .feature-item:hover {
      background: rgba(255,255,255,0.15);
      transform: translateX(5px);
    }
    
    .feature-icon {
      font-size: 24px;
    }
    
    .feature-text {
      font-size: 15px;
      font-weight: 500;
    }
    
    .privacy-notice {
      background: rgba(255,255,255,0.15);
      border-left: 4px solid white;
      padding: 30px;
      border-radius: 12px;
      margin: 40px 0;
      font-size: 15px;
      line-height: 1.8;
      box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .privacy-notice strong {
      font-size: 20px;
      display: block;
      margin-bottom: 15px;
    }
    
    /* 🔴 关键5：确保应用名称在文本中独立显示 */
    .app-name {
      font-weight: 700;
    }
    
    .privacy-links {
      margin-top: 20px;
      display: flex;
      gap: 20px;
      flex-wrap: wrap;
    }
    
    .privacy-link {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: white;
      text-decoration: none;
      background: rgba(255,255,255,0.25);
      padding: 12px 24px;
      border-radius: 10px;
      font-weight: 600;
      font-size: 16px;
      transition: all 0.3s ease;
      border: 2px solid rgba(255,255,255,0.3);
    }
    
    .privacy-link:hover {
      background: rgba(255,255,255,0.35);
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    }
    
    .cta-section {
      text-align: center;
      margin: 60px 0 40px;
    }
    
    .cta-button {
      display: inline-block;
      padding: 18px 40px;
      background: white;
      color: #667eea;
      text-decoration: none;
      border-radius: 30px;
      font-weight: 700;
      font-size: 18px;
      transition: all 0.3s ease;
      box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    .cta-button:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(0,0,0,0.3);
      background: #f0f0f0;
    }
    
    .footer {
      text-align: center;
      padding: 40px 20px;
      border-top: 1px solid rgba(255,255,255,0.2);
      margin-top: 60px;
    }
    
    .footer-links {
      display: flex;
      justify-content: center;
      gap: 30px;
      margin-bottom: 20px;
      flex-wrap: wrap;
    }
    
    .footer-link {
      color: white;
      text-decoration: none;
      font-weight: 500;
      transition: all 0.3s ease;
      padding: 8px 16px;
      border-radius: 8px;
      font-size: 16px;
    }
    
    .footer-link:hover {
      background: rgba(255,255,255,0.1);
      transform: translateY(-2px);
    }
    
    .copyright {
      opacity: 0.8;
      font-size: 14px;
      margin-top: 20px;
      line-height: 1.8;
    }
    
    @keyframes pulse {
      0%, 100% {
        opacity: 1;
      }
      50% {
        opacity: 0.5;
      }
    }
    
    @media (max-width: 768px) {
      .nav-links {
        gap: 10px;
        font-size: 13px;
      }
      
      .nav-link {
        padding: 6px 10px;
      }
      
      .title {
        font-size: 36px;
      }
      
      .description {
        font-size: 16px;
      }
      
      .content {
        grid-template-columns: 1fr;
      }
      
      .features {
        padding: 25px;
      }
      
      .feature-grid {
        grid-template-columns: 1fr;
      }
      
      .footer-links {
        flex-direction: column;
        gap: 15px;
      }
      
      .privacy-links {
        flex-direction: column;
      }
    }
  </style>
</head>
<body>
  <!-- 顶部导航栏 -->
  <nav class="top-nav">
    <div class="top-nav-container">
      <!-- 🔴 关键6：导航品牌名只使用应用名称 -->
      <a href="/" class="nav-brand">星霜的邮件助手</a>
      <div class="nav-links">
        <a href="/privacy" class="nav-link nav-link-privacy" rel="privacy-policy">隐私政策</a>
        <a href="/terms" class="nav-link">服务条款</a>
      </div>
    </div>
  </nav>

  <div class="container">
    <header class="header">
      <div class="logo">📧🤖</div>
      
      <!-- 🔴 🔴 🔴 最关键：H1标签ONLY包含应用名称，不包含任何描述 -->
      <h1 class="title">星霜的邮件助手</h1>
      
      <!-- 🔴 关键7：描述文字独立出来，不放在H1中 -->
      <p class="description">通过 Telegram 安全便捷地访问和管理您的邮箱</p>
      
      <div class="status-badge">
        <span class="status-dot"></span>
        服务正常运行
      </div>
    </header>

    <div class="content">
      <div class="card">
        <div class="card-icon">🔐</div>
        <h2>安全可靠</h2>
        <p>采用 Google OAuth 2.0 授权，数据加密传输，不存储任何邮件内容，完全保护您的隐私。</p>
      </div>

      <div class="card">
        <div class="card-icon">⚡</div>
        <h2>即时同步</h2>
        <p>实时接收新邮件推送通知，随时随地通过 Telegram 查看和管理您的邮箱。</p>
      </div>

      <div class="card">
        <div class="card-icon">🌐</div>
        <h2>多账户支持</h2>
        <p>支持同时管理多个邮箱账户，轻松切换，提高工作效率。</p>
      </div>
    </div>

    <section class="features">
      <h2>🚀 核心功能</h2>
      <div class="feature-grid">
        <div class="feature-item">
          <span class="feature-icon">📬</span>
          <span class="feature-text">查看收件箱</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">🔍</span>
          <span class="feature-text">强大的搜索</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">⭐</span>
          <span class="feature-text">标记星标</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">📊</span>
          <span class="feature-text">邮件统计</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">🔔</span>
          <span class="feature-text">实时推送</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">📎</span>
          <span class="feature-text">附件下载</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">🌐</span>
          <span class="feature-text">网页预览</span>
        </div>
        <div class="feature-item">
          <span class="feature-icon">✅</span>
          <span class="feature-text">批量操作</span>
        </div>
      </div>
    </section>

    <!-- 突出的隐私政策部分 -->
    <div class="privacy-notice">
      <strong>🔒 隐私保护与数据安全</strong>
      <p>
        <!-- 🔴 关键8：正文中使用 <span class="app-name"> 突出应用名称 -->
        <span class="app-name">星霜的邮件助手</span>重视您的隐私。我们不会存储、分享或出售您的任何邮件数据。
        所有数据处理都在加密环境中实时进行，处理完成后立即删除。
        您可以随时撤销授权，删除所有数据。
      </p>
      <p style="margin-top: 15px;">
        了解我们如何保护您的数据，请查看我们的隐私政策和服务条款：
      </p>
      <div class="privacy-links">
        <a href="/privacy" class="privacy-link" rel="privacy-policy">
          📄 隐私政策
        </a>
        <a href="/terms" class="privacy-link">
          📋 服务条款
        </a>
      </div>
    </div>

    <section class="cta-section">
      <a href="tg://resolve" class="cta-button">
        📱 立即在 Telegram 中使用
      </a>
    </section>

    <footer class="footer">
      <nav class="footer-links">
        <a href="/" class="footer-link">首页</a>
        <a href="/privacy" class="footer-link" rel="privacy-policy">隐私政策</a>
        <a href="/terms" class="footer-link">服务条款</a>
        <a href="tg://resolve" class="footer-link">Telegram</a>
        <a href="mailto:xiaobainuli@gmail.com" class="footer-link">联系我们</a>
      </nav>
      
      <div class="copyright">
        <!-- 🔴 关键9：版权声明中的应用名称也要独立、清晰 -->
        <p><span class="app-name">星霜的邮件助手</span> © 2026 - 保留所有权利</p>
        <p style="margin-top: 10px;">
          本服务使用 Google API 服务，遵守 
          <a href="https://developers.google.com/terms/api-services-user-data-policy" 
             target="_blank" 
             rel="noopener noreferrer"
             style="color: white; text-decoration: underline;">
            Google API 服务用户数据政策
          </a>
        </p>
        <p style="margin-top: 10px;">
          <a href="/privacy" rel="privacy-policy" style="color: white; text-decoration: underline;">隐私政策</a> | 
          <a href="/terms" style="color: white; text-decoration: underline;">服务条款</a>
        </p>
      </div>
    </footer>
  </div>
</body>
</html>`;
}


function getResultPage(success, message) {
  const color = success ? '#10b981' : '#ef4444';
  const icon = success ? '✓' : '✕';
  const title = success ? '授权成功' : '授权失败';
  
  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title}</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,-apple-system,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;
background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);padding:20px}
.card{background:rgba(255,255,255,0.1);backdrop-filter:blur(20px);border-radius:24px;padding:48px;text-align:center;
max-width:420px;width:100%;border:1px solid rgba(255,255,255,0.2);box-shadow:0 25px 50px -12px rgba(0,0,0,0.5)}
.icon{width:80px;height:80px;border-radius:50%;background:${color};display:flex;align-items:center;justify-content:center;
font-size:40px;font-weight:bold;color:#fff;margin:0 auto 24px;box-shadow:0 10px 40px ${color}66}
h1{color:#fff;font-size:28px;margin-bottom:16px}
.email{background:rgba(99,102,241,0.2);border:1px solid rgba(99,102,241,0.3);padding:16px 20px;border-radius:12px;
margin:24px 0;word-break:break-all;color:#e0e7ff;font-size:15px}
.btn{display:inline-flex;align-items:center;gap:8px;padding:14px 32px;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
color:#fff;text-decoration:none;border-radius:12px;font-weight:600;font-size:16px;transition:all 0.3s;box-shadow:0 4px 15px rgba(102,126,234,0.4)}
.btn:hover{transform:translateY(-2px);box-shadow:0 6px 20px rgba(102,126,234,0.5)}
.features{display:flex;justify-content:center;gap:24px;margin-top:32px;flex-wrap:wrap}
.feature{display:flex;align-items:center;gap:6px;color:rgba(255,255,255,0.7);font-size:13px}
</style></head>
<body><div class="card">
<div class="icon">${icon}</div>
<h1>${title}</h1>
<div class="email">${success ? '📧 ' + message : '❌ ' + message}</div>
<a href="tg://resolve" class="btn">📱 打开 Telegram</a>
${success ? '<div class="features"><span class="feature">🔒 安全加密</span><span class="feature">⚡ 实时同步</span><span class="feature">🌐 网页预览</span></div>' : ''}
</div></body></html>`;
}

// ==================== 隐私政策页面 ====================
function getPrivacyPage() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>隐私政策 - Gmail Telegram Bot</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      padding: 20px;
    }
    .container {
      max-width: 800px;
      margin: 0 auto;
      background: white;
      border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      overflow: hidden;
    }
    .header {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 40px 30px;
      text-align: center;
    }
    .header h1 {
      font-size: 28px;
      margin-bottom: 10px;
    }
    .header p {
      opacity: 0.9;
      font-size: 14px;
    }
    .content {
      padding: 40px 30px;
      color: #333;
      line-height: 1.8;
    }
    .content h2 {
      color: #667eea;
      margin-top: 30px;
      margin-bottom: 15px;
      font-size: 20px;
    }
    .content h2:first-child {
      margin-top: 0;
    }
    .content p {
      margin-bottom: 15px;
    }
    .content ul {
      margin: 15px 0;
      padding-left: 25px;
    }
    .content li {
      margin-bottom: 10px;
    }
    .highlight {
      background: #f0f4ff;
      padding: 20px;
      border-left: 4px solid #667eea;
      border-radius: 4px;
      margin: 20px 0;
    }
    .footer {
      background: #f8f9fa;
      padding: 30px;
      text-align: center;
      border-top: 1px solid #e0e0e0;
    }
    .footer a {
      color: #667eea;
      text-decoration: none;
      margin: 0 10px;
      font-weight: 500;
    }
    .footer a:hover {
      text-decoration: underline;
    }
    .date {
      color: #999;
      font-size: 14px;
      margin-top: 15px;
    }
    @media (max-width: 600px) {
      .header { padding: 30px 20px; }
      .content { padding: 30px 20px; }
      .header h1 { font-size: 24px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>📧 隐私政策</h1>
      <p>Gmail Telegram Bot - 我们重视您的隐私</p>
    </div>
    
    <div class="content">
      <div class="highlight">
        <strong>简而言之：</strong>我们不会存储、分享或出售您的任何邮件数据。所有数据处理都在加密环境中实时进行，处理完成后立即删除。
      </div>

      <h2>1. 数据收集与使用</h2>
      <p>Gmail Telegram Bot 使用 Google OAuth 2.0 授权访问您的 Gmail 邮箱。我们收集和使用的数据包括：</p>
      <ul>
        <li><strong>Gmail 邮件数据</strong>：用于在 Telegram 中展示邮件列表、内容和附件</li>
        <li><strong>Gmail 账户信息</strong>：邮箱地址，用于识别和管理多个账户</li>
        <li><strong>Telegram 用户ID</strong>：用于关联您的 Telegram 账户与 Gmail 授权</li>
      </ul>

      <h2>2. 数据存储</h2>
      <p>我们采用最小化数据存储原则：</p>
      <ul>
        <li><strong>OAuth Token</strong>：存储在 Cloudflare KV 中，用于访问您的 Gmail（加密存储）</li>
        <li><strong>临时数据</strong>：邮件列表、邮件内容等数据仅在 Cloudflare Workers 运行时内存中临时处理，处理完成后立即删除</li>
        <li><strong>邮件内容</strong>：我们不会永久存储任何邮件内容</li>
        <li><strong>预览链接</strong>：网页预览链接 1 小时后自动失效</li>
      </ul>

      <h2>3. 数据安全</h2>
      <ul>
        <li>所有数据传输使用 HTTPS 加密</li>
        <li>OAuth Token 加密存储在 Cloudflare KV</li>
        <li>应用运行在 Cloudflare 全球安全网络上</li>
        <li>严格的访问控制，只有授权用户才能访问自己的数据</li>
      </ul>

      <h2>4. 数据共享</h2>
      <p><strong>我们绝不会将您的数据出售、出租或分享给第三方。</strong>您的邮件数据仅在以下情况下使用：</p>
      <ul>
        <li>响应您的请求（如查看邮件、搜索、标记等）</li>
        <li>实现应用功能（如邮件推送通知）</li>
      </ul>

      <h2>5. Google API 服务使用</h2>
      <p>Gmail Telegram Bot 使用 Google API 服务，并遵守 <a href="https://developers.google.com/terms/api-services-user-data-policy" target="_blank" style="color: #667eea;">Google API 服务用户数据政策</a>，包括有限使用要求。</p>

      <h2>6. 您的权利</h2>
      <p>您拥有以下权利：</p>
      <ul>
        <li><strong>访问权</strong>：您可以随时通过 Telegram Bot 访问您的数据</li>
        <li><strong>删除权</strong>：您可以在 Bot 中删除账户，我们将立即删除所有相关数据</li>
        <li><strong>撤销授权</strong>：您可以在 <a href="https://myaccount.google.com/permissions" target="_blank" style="color: #667eea;">Google 账户权限设置</a> 中随时撤销应用授权</li>
        <li><strong>数据导出</strong>：您的所有邮件数据始终在您的 Gmail 账户中，可以随时导出</li>
      </ul>

      <h2>7. Cookie 和追踪技术</h2>
      <p>本应用不使用 Cookie、不进行用户追踪、不投放广告。</p>

      <h2>8. 儿童隐私</h2>
      <p>本服务面向 13 岁及以上用户。我们不会故意收集 13 岁以下儿童的个人信息。</p>

      <h2>9. 隐私政策更新</h2>
      <p>我们可能会不时更新本隐私政策。更新后的政策将在本页面发布，重大变更会在 Bot 中通知用户。</p>

      <h2>10. 联系我们</h2>
      <p>如果您对本隐私政策有任何疑问，请通过以下方式联系我们：</p>
      <ul>
        <li>Telegram: 在 Bot 中发送反馈</li>
        <li>Email: xiaobainuli@gmail.com</li>
      </ul>

      <div class="date">
        最后更新日期：2026年1月29日
      </div>
    </div>

    <div class="footer">
      <a href="/">返回首页</a>
      <a href="/terms">服务条款</a>
      <a href="tg://resolve">打开 Telegram</a>
    </div>
  </div>
</body>
</html>`;
}

// ==================== 服务条款页面 ====================
function getTermsPage() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>服务条款 - Gmail Telegram Bot</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      padding: 20px;
    }
    .container {
      max-width: 800px;
      margin: 0 auto;
      background: white;
      border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      overflow: hidden;
    }
    .header {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 40px 30px;
      text-align: center;
    }
    .header h1 {
      font-size: 28px;
      margin-bottom: 10px;
    }
    .header p {
      opacity: 0.9;
      font-size: 14px;
    }
    .content {
      padding: 40px 30px;
      color: #333;
      line-height: 1.8;
    }
    .content h2 {
      color: #667eea;
      margin-top: 30px;
      margin-bottom: 15px;
      font-size: 20px;
    }
    .content h2:first-child {
      margin-top: 0;
    }
    .content p {
      margin-bottom: 15px;
    }
    .content ul {
      margin: 15px 0;
      padding-left: 25px;
    }
    .content li {
      margin-bottom: 10px;
    }
    .highlight {
      background: #fff3cd;
      padding: 20px;
      border-left: 4px solid #ffc107;
      border-radius: 4px;
      margin: 20px 0;
    }
    .footer {
      background: #f8f9fa;
      padding: 30px;
      text-align: center;
      border-top: 1px solid #e0e0e0;
    }
    .footer a {
      color: #667eea;
      text-decoration: none;
      margin: 0 10px;
      font-weight: 500;
    }
    .footer a:hover {
      text-decoration: underline;
    }
    .date {
      color: #999;
      font-size: 14px;
      margin-top: 15px;
    }
    @media (max-width: 600px) {
      .header { padding: 30px 20px; }
      .content { padding: 30px 20px; }
      .header h1 { font-size: 24px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>📜 服务条款</h1>
      <p>Gmail Telegram Bot - 使用条款与协议</p>
    </div>
    
    <div class="content">
      <div class="highlight">
        <strong>重要提示：</strong>使用本服务即表示您同意这些条款。如果您不同意，请不要使用本服务。
      </div>

      <h2>1. 服务说明</h2>
      <p>Gmail Telegram Bot（以下简称"本服务"）是一个通过 Telegram 访问和管理 Gmail 邮箱的工具。本服务允许您：</p>
      <ul>
        <li>通过 Telegram 查看 Gmail 邮件</li>
        <li>搜索、标记、删除邮件</li>
        <li>接收新邮件推送通知</li>
        <li>管理多个 Gmail 账户</li>
      </ul>

      <h2>2. 使用资格</h2>
      <ul>
        <li>您必须年满 13 岁才能使用本服务</li>
        <li>您必须拥有有效的 Google 账户和 Telegram 账户</li>
        <li>您必须遵守 Google 和 Telegram 的服务条款</li>
      </ul>

      <h2>3. 账户安全</h2>
      <p>您有责任：</p>
      <ul>
        <li>保护您的 Telegram 账户安全</li>
        <li>不与他人共享您的授权访问</li>
        <li>发现未经授权的访问时立即撤销授权</li>
        <li>定期检查 <a href="https://myaccount.google.com/permissions" target="_blank" style="color: #667eea;">Google 账户权限</a></li>
      </ul>

      <h2>4. 可接受使用</h2>
      <p>您同意不会：</p>
      <ul>
        <li>使用本服务进行非法活动</li>
        <li>尝试破解、反向工程或干扰本服务</li>
        <li>滥用服务资源（如过度请求）</li>
        <li>访问他人的 Gmail 账户</li>
        <li>利用本服务发送垃圾邮件或恶意内容</li>
      </ul>

      <h2>5. 服务限制</h2>
      <ul>
        <li>本服务可能会有使用频率限制</li>
        <li>某些功能可能需要额外的 Google API 配额</li>
        <li>我们保留随时修改、暂停或终止服务的权利</li>
        <li>服务可能因维护而暂时不可用</li>
      </ul>

      <h2>6. 免责声明</h2>
      <p><strong>本服务按"原样"提供，不提供任何明示或暗示的保证。</strong>我们不保证：</p>
      <ul>
        <li>服务将不间断或无错误</li>
        <li>服务满足您的特定需求</li>
        <li>通过服务获取的结果准确或可靠</li>
      </ul>

      <h2>7. 责任限制</h2>
      <p>在法律允许的最大范围内：</p>
      <ul>
        <li>我们不对任何间接、偶然、特殊或后果性损害负责</li>
        <li>我们不对数据丢失、业务中断或利润损失负责</li>
        <li>您使用本服务的风险由您自行承担</li>
      </ul>

      <h2>8. 知识产权</h2>
      <ul>
        <li>本服务的所有权利归开发者所有</li>
        <li>您的邮件内容归您所有</li>
        <li>我们不会声称对您的数据拥有任何权利</li>
      </ul>

      <h2>9. 第三方服务</h2>
      <p>本服务依赖以下第三方服务：</p>
      <ul>
        <li><strong>Google Gmail API</strong>：受 <a href="https://developers.google.com/terms" target="_blank" style="color: #667eea;">Google API 服务条款</a> 约束</li>
        <li><strong>Telegram Bot API</strong>：受 <a href="https://telegram.org/tos" target="_blank" style="color: #667eea;">Telegram 服务条款</a> 约束</li>
        <li><strong>Cloudflare Workers</strong>：服务托管平台</li>
      </ul>
      <p>我们对这些第三方服务不承担责任。</p>

      <h2>10. 终止使用</h2>
      <p>您可以随时停止使用本服务：</p>
      <ul>
        <li>在 Bot 中删除您的账户</li>
        <li>在 <a href="https://myaccount.google.com/permissions" target="_blank" style="color: #667eea;">Google 账户设置</a> 中撤销授权</li>
      </ul>
      <p>我们也可能在以下情况下终止您的访问：</p>
      <ul>
        <li>您违反了这些服务条款</li>
        <li>您滥用服务资源</li>
        <li>法律要求</li>
      </ul>

      <h2>11. 条款修改</h2>
      <p>我们保留随时修改这些条款的权利。重大变更会通过 Bot 通知用户。继续使用服务即表示您接受修改后的条款。</p>

      <h2>12. 适用法律</h2>
      <p>这些条款受中华人民共和国法律管辖。</p>

      <h2>13. 联系方式</h2>
      <p>如有任何疑问，请联系：</p>
      <ul>
        <li>Telegram: 在 Bot 中发送反馈</li>
        <li>Email: xiaobainuli@gmail.com</li>
      </ul>

      <div class="date">
        最后更新日期：2026年1月29日
      </div>
    </div>

    <div class="footer">
      <a href="/">返回首页</a>
      <a href="/privacy">隐私政策</a>
      <a href="tg://resolve">打开 Telegram</a>
    </div>
  </div>
</body>
</html>`;
}