//! Attachment loading shared by adapters: absolutize, read, sniff, inline.
//!
//! The rule (proven by Comet, T3, and laptop-agent): every attachment rides
//! the prompt text as a path ref — any file type works because the agent can
//! open it with its own tools. Images under the cap are additionally inlined
//! as base64 blocks so vision works without a tool call. Nothing here is
//! fatal: an unreadable file becomes a problem report, everything else
//! degrades to the path ref.

use std::path::PathBuf;

/// Above this, an image is not inlined and rides as a path ref only
/// (Comet's cap, verified against the real claude CLI).
const INLINE_CAP: usize = 5 * 1024 * 1024;

/// One attachment after loading.
pub(crate) struct Loaded {
    /// Absolute path, for the text ref.
    pub path: String,
    /// Present when the file is an image under the inline cap.
    pub image: Option<Inline>,
    /// Present when the file could not be read.
    pub problem: Option<String>,
}

pub(crate) struct Inline {
    pub mime: &'static str,
    pub base64: String,
}

/// Reads each attachment; sniffs images and encodes those under the cap.
pub(crate) async fn load(paths: &[PathBuf]) -> Vec<Loaded> {
    let mut loaded = Vec::with_capacity(paths.len());
    for path in paths {
        let absolute = std::path::absolute(path).unwrap_or_else(|_| path.clone());
        let path = absolute.display().to_string();
        match tokio::fs::read(&absolute).await {
            Ok(bytes) => loaded.push(Loaded {
                path,
                image: sniff(&bytes)
                    .filter(|_| bytes.len() <= INLINE_CAP)
                    .map(|mime| Inline {
                        mime,
                        base64: base64(&bytes),
                    }),
                problem: None,
            }),
            Err(error) => loaded.push(Loaded {
                problem: Some(format!("attachment unreadable ({path}): {error}")),
                path,
                image: None,
            }),
        }
    }
    loaded
}

/// The prompt text with a trailer naming every attached path.
pub(crate) fn with_refs(text: &str, loaded: &[Loaded]) -> String {
    if loaded.is_empty() {
        return text.to_owned();
    }
    let refs: Vec<String> = loaded.iter().map(|l| format!("- {}", l.path)).collect();
    format!("{text}\n\nAttached files:\n{}", refs.join("\n"))
}

/// Image mime by magic bytes; anything else is not inlined.
fn sniff(bytes: &[u8]) -> Option<&'static str> {
    match bytes {
        [0x89, b'P', b'N', b'G', ..] => Some("image/png"),
        [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg"),
        [b'G', b'I', b'F', b'8', ..] => Some("image/gif"),
        [
            b'R',
            b'I',
            b'F',
            b'F',
            _,
            _,
            _,
            _,
            b'W',
            b'E',
            b'B',
            b'P',
            ..,
        ] => Some("image/webp"),
        _ => None,
    }
}

/// Standard base64 with padding. Encoding only, so a dependency is not worth it.
pub(crate) fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        let chars = [n >> 18, n >> 12, n >> 6, n];
        for (i, c) in chars.iter().enumerate() {
            if i <= chunk.len() {
                out.push(TABLE[(c & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// base64 encodes RFC4648 vectors correctly with padding.
    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64(&[0xFF, 0x00, 0xFF]), "/wD/");
    }

    /// sniff detects PNG/JPEG/GIF/WEBP magic bytes and returns None for PDF/empty.
    #[test]
    fn sniff_knows_the_inline_formats() {
        assert_eq!(sniff(b"\x89PNG\r\n\x1a\n"), Some("image/png"));
        assert_eq!(sniff(&[0xFF, 0xD8, 0xFF, 0xE0]), Some("image/jpeg"));
        assert_eq!(sniff(b"GIF89a"), Some("image/gif"));
        assert_eq!(sniff(b"RIFF\x00\x00\x00\x00WEBP"), Some("image/webp"));
        assert_eq!(sniff(b"%PDF-1.7"), None);
        assert_eq!(sniff(b""), None);
    }

    /// load inlines small PNG, leaves PDF as ref, and reports unreadable/missing files with problem.
    #[tokio::test]
    async fn load_inlines_images_and_reports_unreadable_files() {
        let dir = std::env::temp_dir().join(format!("anyagent-attach-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.png"), b"\x89PNG\r\n\x1a\ndata").unwrap();
        std::fs::write(dir.join("b.pdf"), b"%PDF-1.7 data").unwrap();
        let paths = [dir.join("a.png"), dir.join("b.pdf"), dir.join("missing")];
        let loaded = load(&paths).await;
        assert_eq!(loaded[0].image.as_ref().unwrap().mime, "image/png");
        assert!(loaded[1].image.is_none() && loaded[1].problem.is_none());
        assert!(loaded[2].problem.is_some());
        let text = with_refs("look", &loaded);
        assert!(text.starts_with("look\n\nAttached files:\n- "));
        assert!(text.contains("b.pdf"));
    }
}
