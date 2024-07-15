use once_cell::sync::Lazy;
use regex::Regex;

static RE_IMG: Lazy<Regex> =
    Lazy::new(|| Regex::new("\\[??!??\\[([^\\]]*)\\]\\([^)]*\\)").unwrap());

fn line_clean(line: &str) -> Option<String> {
    let tl = line.trim();
    if tl.len() > 5 && tl.ends_with('。') {
        Some(line.to_string())
    } else {
        None
    }
}

pub fn clean_seq(s: &str) -> Vec<String> {
    let mut s = s.to_string();
    for _ in 0..5 {
        s = RE_IMG.replace_all(&s, "$1").to_string();
    }
    let mut r = vec![];
    let mut cur = String::new();
    let mut sep = 0;
    for s in s.split('\n').map(line_clean) {
        if let Some(s) = s {
            if sep > 8 && !cur.is_empty() {
                r.push(cur.trim().to_string());
                cur.clear();
            }
            cur.push_str(&format!("\n{}", s));
            sep = 0;
        } else {
            sep += 1;
        }
    }
    r
}
