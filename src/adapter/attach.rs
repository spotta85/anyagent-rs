//! Attachment loading shared by adapters: absolutize, read, sniff, inline.
//!
//! The rule (proven by Comet, T3, and laptop-agent): every attachment rides
//! the prompt text as a path ref — any file type works because the agent can
//! open it with its own tools. Images, audio, and PDFs under the cap are
//! additionally kept inline so a wire that takes them gets the bytes without
//! a tool call; each wire encodes only what it sends. Nothing here is fatal:
//! an unreadable file becomes a problem report, everything else degrades to
//! the path ref.

use std::path::PathBuf;

/// Above this, a file is not inlined and rides as a path ref only
/// (Comet's cap, verified against the real claude CLI).
const INLINE_CAP: usize = 5 * 1024 * 1024;

/// One attachment after loading.
pub(crate) struct Loaded {
    /// Absolute path, for the text ref.
    pub path: String,
    /// Present when the file is a known media type under the inline cap.
    pub inline: Option<Inline>,
    /// Present when the file could not be read.
    pub problem: Option<String>,
}

impl Loaded {
    /// The inlined bytes when they are an image (every wire takes those).
    pub fn image(&self) -> Option<&Inline> {
        self.inline.as_ref().filter(|i| i.media == Media::Image)
    }
}

pub(crate) struct Inline {
    pub media: Media,
    pub mime: &'static str,
    pub bytes: Vec<u8>,
}

impl Inline {
    /// The bytes as standard base64, for the wires that take them so.
    pub fn base64(&self) -> String {
        base64(&self.bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Media {
    Image,
    Audio,
    Pdf,
}

/// Reads each attachment up to the cap and sniffs the media it holds.
/// Anything past the cap rides as a path ref only, however large the file.
pub(crate) async fn load(paths: &[PathBuf]) -> Vec<Loaded> {
    let mut loaded = Vec::with_capacity(paths.len());
    for path in paths {
        let absolute = std::path::absolute(path).unwrap_or_else(|_| path.clone());
        let path = absolute.display().to_string();
        match read_capped(&absolute).await {
            Ok(bytes) => loaded.push(Loaded {
                path,
                inline: sniff(&bytes)
                    .filter(|_| bytes.len() <= INLINE_CAP)
                    .map(|(media, mime)| Inline { media, mime, bytes }),
                problem: None,
            }),
            Err(error) => loaded.push(Loaded {
                problem: Some(format!("attachment unreadable ({path}): {error}")),
                path,
                inline: None,
            }),
        }
    }
    loaded
}

/// At most `INLINE_CAP + 1` bytes of the file: one over marks it as too big.
async fn read_capped(path: &PathBuf) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await?
        .take(INLINE_CAP as u64 + 1)
        .read_to_end(&mut bytes)
        .await?;
    Ok(bytes)
}

/// The prompt text with a trailer naming every attached path.
pub(crate) fn with_refs(text: &str, loaded: &[Loaded]) -> String {
    if loaded.is_empty() {
        return text.to_owned();
    }
    let refs: Vec<String> = loaded.iter().map(|l| format!("- {}", l.path)).collect();
    format!("{text}\n\nAttached files:\n{}", refs.join("\n"))
}

/// Media kind and mime by magic bytes; anything else is not inlined.
fn sniff(bytes: &[u8]) -> Option<(Media, &'static str)> {
    use Media::*;
    let riff = |tag: &[u8; 4]| bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == tag;
    Some(match bytes {
        [0x89, b'P', b'N', b'G', ..] => (Image, "image/png"),
        [0xFF, 0xD8, 0xFF, ..] => (Image, "image/jpeg"),
        [b'G', b'I', b'F', b'8', ..] => (Image, "image/gif"),
        _ if riff(b"WEBP") => (Image, "image/webp"),
        _ if riff(b"WAVE") => (Audio, "audio/wav"),
        // ID3 tag, or a bare frame: 11 sync bits, a valid MPEG version
        // (the 01 pattern is reserved), layer III, with or without CRC
        // (FB, FA, F3, F2, E3, E2).
        [b'I', b'D', b'3', ..] => (Audio, "audio/mpeg"),
        [0xFF, b, ..] if b & 0xE6 == 0xE2 && b & 0x18 != 0x08 => (Audio, "audio/mpeg"),
        [b'O', b'g', b'g', b'S', ..] => (Audio, "audio/ogg"),
        [b'f', b'L', b'a', b'C', ..] => (Audio, "audio/flac"),
        [_, _, _, _, b'f', b't', b'y', b'p', b'M', b'4', b'A', ..] => (Audio, "audio/mp4"),
        [b'%', b'P', b'D', b'F', ..] => (Pdf, "application/pdf"),
        _ => return None,
    })
}

/// The absolute path as a `file:` URI: everything outside the unreserved
/// set and `/` is percent-encoded, so a space or `#` in a name survives.
pub(crate) fn file_uri(path: &str) -> String {
    let mut uri = String::from("file://");
    for b in path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                uri.push(b as char)
            }
            _ => uri.push_str(&format!("%{b:02X}")),
        }
    }
    uri
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

    /// file_uri keeps unreserved characters and encodes the rest.
    #[test]
    fn file_uri_encodes_reserved_characters() {
        assert_eq!(file_uri("/tmp/a-b_c.pdf"), "file:///tmp/a-b_c.pdf");
        assert_eq!(file_uri("/tmp/a #1.pdf"), "file:///tmp/a%20%231.pdf");
        assert_eq!(file_uri("/tmp/é.pdf"), "file:///tmp/%C3%A9.pdf");
    }

    /// sniff detects image, audio, and PDF magic bytes; anything else is None.
    #[test]
    fn sniff_knows_the_inline_formats() {
        assert_eq!(
            sniff(b"\x89PNG\r\n\x1a\n"),
            Some((Media::Image, "image/png"))
        );
        assert_eq!(
            sniff(&[0xFF, 0xD8, 0xFF, 0xE0]),
            Some((Media::Image, "image/jpeg"))
        );
        assert_eq!(sniff(b"GIF89a"), Some((Media::Image, "image/gif")));
        assert_eq!(
            sniff(b"RIFF\x00\x00\x00\x00WEBP"),
            Some((Media::Image, "image/webp"))
        );
        assert_eq!(
            sniff(b"RIFF\x00\x00\x00\x00WAVEfmt "),
            Some((Media::Audio, "audio/wav"))
        );
        assert_eq!(sniff(b"ID3\x04"), Some((Media::Audio, "audio/mpeg")));
        assert_eq!(
            sniff(&[0xFF, 0xFA, 0x90, 0x00]),
            Some((Media::Audio, "audio/mpeg"))
        );
        assert_eq!(
            sniff(&[0xFF, 0xFB, 0x90, 0x00]),
            Some((Media::Audio, "audio/mpeg")),
            "CRC-protected frames are mp3"
        );
        assert_eq!(
            sniff(&[0xFF, 0xEA, 0x90, 0x00]),
            None,
            "reserved MPEG version is not mp3"
        );
        assert_eq!(
            sniff(&[0xFF, 0xFD, 0x90, 0x00]),
            None,
            "layer II is not mp3"
        );
        assert_eq!(sniff(b"OggS"), Some((Media::Audio, "audio/ogg")));
        assert_eq!(
            sniff(b"\x00\x00\x00\x20ftypM4A "),
            Some((Media::Audio, "audio/mp4"))
        );
        assert_eq!(sniff(b"%PDF-1.7"), Some((Media::Pdf, "application/pdf")));
        assert_eq!(sniff(b"RIFF\x00\x00"), None);
        assert_eq!(sniff(b"hello"), None);
        assert_eq!(sniff(b""), None);
    }

    /// load inlines a small PNG as an image and a PDF as a pdf, keeps both as refs, and reports unreadable files.
    #[tokio::test]
    async fn load_inlines_images_and_reports_unreadable_files() {
        let dir = std::env::temp_dir().join(format!("anyagent-attach-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.png"), b"\x89PNG\r\n\x1a\ndata").unwrap();
        std::fs::write(dir.join("b.pdf"), b"%PDF-1.7 data").unwrap();
        let paths = [dir.join("a.png"), dir.join("b.pdf"), dir.join("missing")];
        let loaded = load(&paths).await;
        assert_eq!(loaded[0].image().unwrap().mime, "image/png");
        assert!(loaded[1].image().is_none() && loaded[1].problem.is_none());
        assert_eq!(loaded[1].inline.as_ref().unwrap().media, Media::Pdf);
        assert!(loaded[2].problem.is_some());
        let text = with_refs("look", &loaded);
        assert!(text.starts_with("look\n\nAttached files:\n- "));
        assert!(text.contains("b.pdf"));
    }
}
