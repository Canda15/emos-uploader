use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs::File as StdFile;
use std::io::Write;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

/// 本地时间格式化器，使用 VPS 当前时区
struct LocalTimer;
impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"))
    }
}

/// 命令行参数定义
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 视频文件路径
    #[arg(short, long)]
    file: String,

    /// Authorization Token (例如: 1063_gXKWD9Vk3HLso9LB)
    #[arg(short, long)]
    auth: String,

    /// 资源类型和ID组合 (例如: vl-1234 或 ve-5678)
    #[arg(short, long)]
    item: String,

    /// 并发上传线程数 (默认 1，OneDrive官方推荐单线程)
    #[arg(short, long, default_value_t = 1)]
    threads: usize,

    /// 限制最大上传速度，单位: Mbps (选填)
    #[arg(short, long)]
    speed: Option<f64>,

    /// 分片大小，单位: MB。OneDrive要求必须是320KB的倍数，程序会自动向下取整对齐。(默认: 30)
    #[arg(short = 'c', long = "chunk-size", default_value_t = 30)]
    chunk_size: u64,
}

#[derive(Serialize)]
struct TokenRequest<'a> {
    #[serde(rename = "type")]
    req_type: &'a str,
    file_type: &'a str,
    file_name: &'a str,
    file_size: u64,
    file_storage: &'a str,
}

#[derive(Deserialize, Debug)]
struct TokenResponse {
    file_id: String,
    data: TokenData,
}

#[derive(Deserialize, Debug)]
struct TokenData {
    upload_url: String,
}

#[derive(Serialize)]
struct SaveRequest<'a> {
    item_type: &'a str,
    item_id: &'a str,
    file_id: &'a str,
}

/// 定义分片结构体
#[derive(Clone)]
struct Chunk {
    start: u64,
    end: u64,
    size: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_target(false)
        .init();

    let args = Args::parse();

    // 1. 解析 item 参数 (vl-1234)
    let parts: Vec<&str> = args.item.split('-').collect();
    if parts.len() != 2 || (parts[0] != "vl" && parts[0] != "ve") {
        anyhow::bail!("--item 参数格式错误，必须为 vl-xxxx 或 ve-xxxx 的形式！");
    }
    let item_type = parts[0];
    let item_id = parts[1];

    let path = Path::new(&args.file);
    if !path.exists() {
        anyhow::bail!("找不到文件: {}", args.file);
    }
    let file_name = path.file_name().unwrap().to_string_lossy();
    let file_size = path.metadata()?.len();
    let mime_type = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();

    let auth_header = if args.auth.starts_with("Bearer ") {
        args.auth.clone()
    } else {
        format!("Bearer {}", args.auth)
    };

    // 构建客户端，使用 rustls，自动协商 HTTP/2
    let client = Client::builder()
        .use_rustls_tls()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")
        .build()?;

    // ==========================================
    // 步骤 0: 获取基本信息 (展示给用户确认)
    // ==========================================
    info!("正在获取目标资源基本信息...");
    let base_url = format!("https://emos.best/api/upload/video/base?item_type={}&item_id={}", item_type, item_id);
    let base_res = client.get(&base_url).header("Authorization", &auth_header).send().await?;
    if base_res.status().is_success() {
        let base_info = base_res.json::<serde_json::Value>().await?;
        if let Some(title) = base_info.get("title").and_then(|t| t.as_str()) {
            info!("🎯 目标视频信息确认: {}", title);
        }
    } else {
        warn!("无法获取基础信息，但仍将尝试上传。HTTP状态码: {}", base_res.status());
    }

    // ==========================================
    // 步骤 1: 申请上传 Token 及 OneDrive 链接
    // ==========================================
    info!("开始申请上传 Token...");
    let token_req = TokenRequest {
        req_type: "video",
        file_type: &mime_type,
        file_name: &file_name,
        file_size,
        file_storage: "default",
    };

    let token_res = client
        .post("https://emos.best/api/upload/getUploadToken")
        .header("Authorization", &auth_header)
        .json(&token_req)
        .send()
        .await?
        .error_for_status()?
        .json::<TokenResponse>()
        .await?;

    let file_id = token_res.file_id;
    let upload_url = token_res.data.upload_url;
    info!("Token 获取成功! file_id: {}", file_id);

    // ==========================================
    // 步骤 2: 多线程并发分片上传至 OneDrive
    // ==========================================
        // OneDrive 要求分片必须是 320 KB 的倍数 (327,680 字节)
    let chunk_multiple: u64 = 327_680;
    
    // 1. 将用户输入的 MB 转换为 Bytes
    let requested_bytes = args.chunk_size * 1024 * 1024;
    
    // 2. 利用整数除法的特性，自动向下对齐到 320KB 的倍数
    let mut chunk_size = (requested_bytes / chunk_multiple) * chunk_multiple;
    
    // 3. 安全兜底：防止用户输入 0 导致分片大小为 0，最小限制为 320 KB
    if chunk_size == 0 {
        chunk_size = chunk_multiple;
    }

    info!("设定的分片大小为 {} MB，为满足 OneDrive 限制，已自动对齐为 {} Bytes", args.chunk_size, chunk_size);
    
    let mut chunks = Vec::new();
    let mut start: u64 = 0;

    while start < file_size {
        let mut end = start + chunk_size - 1;
        if end >= file_size {
            end = file_size - 1;
        }
        chunks.push(Chunk { start, end, size: end - start + 1 });
        start = end + 1;
    }

    // 计算限速。将总限速 (Bytes/s) 平分给所有活动的线程
    let total_limit_bps = args.speed.unwrap_or(0.0) * 1_000_000.0 / 8.0;
    let thread_limit_bps = if total_limit_bps > 0.0 {
        total_limit_bps / (args.threads as f64)
    } else {
        0.0
    };

    if total_limit_bps > 0.0 {
        info!("已启用限速: {} Mbps，分配到 {} 个线程", args.speed.unwrap(), args.threads);
    }
    info!("即将上传 {} 个分片，采用 HTTP/2 协议多路复用...", chunks.len());

    // 构建并发流执行上传
    let upload_tasks = futures::stream::iter(chunks).map(|chunk| {
        let client = client.clone();
        let upload_url = upload_url.clone();
        let file_path = args.file.clone();

        async move {
            info!("==> 线程开始上传分片: {} - {} / {}", chunk.start, chunk.end, file_size);

            let stream = rate_limited_chunk_stream(file_path, chunk.start, chunk.size, thread_limit_bps);
            let body = reqwest::Body::wrap_stream(stream);
            let range_header = format!("bytes {}-{}/{}", chunk.start, chunk.end, file_size);

            let res = client
                .put(&upload_url)
                .header("Content-Length", chunk.size.to_string())
                .header("Content-Range", range_header)
                .header("Content-Type", "application/octet-stream")
                .body(body)
                .send()
                .await?;

            if res.status().is_success() || res.status().as_u16() == 202 {
                info!("<== 分片 {} - {} 上传成功", chunk.start, chunk.end);
                Ok(())
            } else {
                let status = res.status();
                let text = res.text().await.unwrap_or_default();
                // ⭐ 这里修改了报错信息，精确显示是哪个范围发生了错误
                anyhow::bail!("分片 {} - {} 上传失败: HTTP {} - {}", chunk.start, chunk.end, status, text)
            }
        }
    });

    // ⭐ 替换为快速失败 (Fail-Fast) 机制：
    // 不再使用 collect().await 傻等，只要 Stream 里吐出任何一个 Error，立刻打断循环，让整个程序报警退出！
    let mut upload_tasks_stream = upload_tasks.buffer_unordered(args.threads);
    while let Some(result) = upload_tasks_stream.next().await {
        result?; 
    }
    
    info!("文件已成功上传至 OneDrive!");

    // ==========================================
    // 步骤 3: 最终通知主站保存 (带自动重试及人工介入)
    // ==========================================
    let save_req = SaveRequest {
        item_type,
        item_id,
        file_id: &file_id,
    };

    let mut auto_retry = true;

    loop {
        info!("正在提交最终保存请求...");
        let res = client
            .post("https://emos.best/api/upload/video/save")
            .header("Authorization", &auth_header)
            .json(&save_req)
            .send()
            .await;

        match res {
            Ok(r) if r.status().is_success() => {
                let body = r.json::<serde_json::Value>().await.unwrap_or_default();
                let carrot = body.get("carrot").map(|v| v.to_string()).unwrap_or_else(|| "0".to_string());
                let media_id = body.get("media_id").and_then(|m| m.as_str()).unwrap_or("未知");
                
                info!("🎉 恭喜！视频保存成功！\n获得胡萝卜: {}\n分配的媒体 ID: {}", carrot, media_id);
                break;
            }
            Ok(r) => {
                let status = r.status();
                let error_info = r.json::<serde_json::Value>().await.unwrap_or_default();
                let message = error_info.get("message").and_then(|m| m.as_str()).unwrap_or("未知错误");
                error!("保存失败 (HTTP {}): {}", status, message);
            }
            Err(e) => {
                error!("保存请求引发网络异常: {}", e);
            }
        }

        if auto_retry {
            warn!("这是第一次失败，程序将在 5 秒后自动尝试重试...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            auto_retry = false; // 取消后续自动重试
        } else {
            print!("再次保存仍然失败，是否需要再试一次？ (y/n): ");
            std::io::stdout().flush()?; // 刷新控制台使得提示符立即可见

            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;

            if input.trim().eq_ignore_ascii_case("y") {
                info!("人工确认，开始新一轮重试...");
            } else {
                error!("用户取消保存，程序退出。");
                break;
            }
        }
    }

    Ok(())
}

/// 构造一个读取文件片段且带有平滑限速的异步 Stream
fn rate_limited_chunk_stream(
    file_path: String,
    start: u64,
    len: u64,
    limit_bps: f64,
) -> impl futures::stream::Stream<Item = std::io::Result<Bytes>> {
    async_stream::try_stream! {
        // 每个线程独立打开文件，允许并发 Seek 读取
        let mut file = File::from_std(StdFile::open(&file_path)?);
        file.seek(tokio::io::SeekFrom::Start(start)).await?;

        let mut buffer = vec![0; 64 * 1024]; // 64KB 缓冲区
        let mut bytes_read_total = 0;
        let start_time = Instant::now();

        while bytes_read_total < len {
            let to_read = std::cmp::min(buffer.len() as u64, len - bytes_read_total) as usize;
            let n = file.read(&mut buffer[..to_read]).await?;
            if n == 0 {
                break;
            }
            bytes_read_total += n as u64;
            yield Bytes::copy_from_slice(&buffer[..n]);

            // 执行限速睡眠判定
            if limit_bps > 0.0 {
                let expected_time = Duration::from_secs_f64(bytes_read_total as f64 / limit_bps);
                let elapsed = start_time.elapsed();
                if elapsed < expected_time {
                    tokio::time::sleep(expected_time - elapsed).await;
                }
            }
        }
    }
}
