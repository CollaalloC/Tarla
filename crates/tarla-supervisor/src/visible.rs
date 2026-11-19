//! Some OpenAI-compatible local models put marked thought blocks in otherwise
//! public content. Hold partial tags across transport events so they cannot leak.
#[derive(Default)]
pub(super) struct VisibleText {
    pub text: String,
    pending: String,
    closing: Option<&'static str>,
}
impl VisibleText {
    pub fn append(&mut self, chunk: &str) {
        self.pending.push_str(chunk);
        loop {
            if let Some(close) = self.closing {
                if let Some(at) = self.pending.find(close) {
                    self.pending.drain(..at + close.len());
                    self.closing = None;
                    continue;
                }
                let keep = partial(&self.pending, &[close]);
                let discard = self.pending.len() - keep;
                self.pending.drain(..discard);
                return;
            }
            let tags = [("<think>", "</think>"), ("<analysis>", "</analysis>")];
            if let Some((at, open, close)) = tags
                .iter()
                .filter_map(|(a, b)| self.pending.find(a).map(|i| (i, *a, *b)))
                .min_by_key(|(i, _, _)| *i)
            {
                self.text.push_str(&self.pending[..at]);
                self.pending.drain(..at + open.len());
                self.closing = Some(close);
                continue;
            }
            let keep = partial(&self.pending, &["<think>", "<analysis>"]);
            let emit = self.pending.len() - keep;
            self.text.push_str(&self.pending[..emit]);
            self.pending.drain(..emit);
            return;
        }
    }
    pub fn finish(self) -> String {
        let mut text = self.text;
        if self.closing.is_none() {
            text.push_str(&self.pending)
        }
        text
    }
}
fn partial(s: &str, tags: &[&str]) -> usize {
    tags.iter()
        .flat_map(|tag| (1..tag.len()).map(move |n| &tag[..n]))
        .filter(|prefix| s.ends_with(*prefix))
        .map(str::len)
        .max()
        .unwrap_or(0)
}
pub(super) fn public_string(s: &str) -> String {
    let mut visible = VisibleText::default();
    visible.append(s);
    visible.finish()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_inline_thoughts_never_enter_preview() {
        let mut s = VisibleText::default();
        let word = "开始<think>private plan</think>α<analysis>hidden tool values</analysis>é!";
        for ch in word.chars() {
            s.append(&ch.to_string());
            assert!("开始αé!".starts_with(&s.text));
        }
        assert_eq!(s.finish(), "开始αé!");
        assert_eq!(public_string("value < threshold"), "value < threshold");
        assert_eq!(public_string("<think>incomplete"), "");
    }
}
