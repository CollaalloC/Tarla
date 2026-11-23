//! Strict, zero-external-dependency YAML 1.2 subset parser for `botflow.yml`.
//! Supports mappings, sequences (including inline map items `- id: foo`), flow arrays/maps,
//! quoted/unquoted scalars, booleans, numbers, comments, and literal/folded block scalars.
use anyhow::{bail, ensure, Context, Result};
use serde_json::{Map, Number, Value};

#[derive(Debug, Clone)]
struct Line<'a> {
    no: usize,
    indent: usize,
    text: &'a str,
}

pub fn parse_yaml_to_json(input: &str) -> Result<Value> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("botflow.yml is empty");
    }
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).context("invalid JSON document");
    }
    let mut lines = Vec::new();
    for (idx, raw) in input.lines().enumerate() {
        let line_no = idx + 1;
        ensure!(
            !raw.contains('\t'),
            "line {line_no}: tabs are not allowed for YAML indentation"
        );
        let stripped = strip_comment(raw);
        let content = stripped.trim_end();
        let indent = content.bytes().take_while(|b| *b == b' ').count();
        let rest = &content[indent..];
        if rest.is_empty() || (indent == 0 && rest == "---") {
            continue;
        }
        lines.push(Line {
            no: line_no,
            indent,
            text: rest,
        });
    }
    ensure!(!lines.is_empty(), "botflow.yml contains no configuration");
    let mut pos = 0;
    let root_indent = lines[0].indent;
    let val = parse_node(&lines, &mut pos, root_indent)?;
    if pos < lines.len() {
        bail!(
            "line {}: unexpected trailing content `{}`",
            lines[pos].no,
            lines[pos].text
        );
    }
    Ok(val)
}

fn strip_comment(line: &str) -> &str {
    let mut in_single = false;
    let mut in_double = false;
    let mut prev_escape = false;
    for (idx, ch) in line.char_indices() {
        if in_double {
            if prev_escape {
                prev_escape = false;
            } else if ch == '\\' {
                prev_escape = true;
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        match ch {
            '"' => in_double = true,
            '\'' => in_single = true,
            '#' if idx == 0 || line.as_bytes()[idx - 1].is_ascii_whitespace() => {
                return &line[..idx];
            }
            _ => {}
        }
    }
    line
}

fn parse_node(lines: &[Line<'_>], pos: &mut usize, indent: usize) -> Result<Value> {
    let line = &lines[*pos];
    ensure!(
        line.indent == indent,
        "line {}: unexpected indentation (expected {indent}, got {})",
        line.no,
        line.indent
    );
    if is_seq_item(line.text) {
        parse_sequence(lines, pos, indent)
    } else {
        parse_mapping(lines, pos, indent)
    }
}

fn is_seq_item(text: &str) -> bool {
    text == "-" || text.starts_with("- ")
}

fn parse_sequence(lines: &[Line<'_>], pos: &mut usize, indent: usize) -> Result<Value> {
    let mut items = Vec::new();
    while *pos < lines.len() {
        let line = &lines[*pos];
        if line.indent < indent {
            break;
        }
        ensure!(
            line.indent == indent,
            "line {}: misaligned sequence item",
            line.no
        );
        if !is_seq_item(line.text) {
            break;
        }
        let rest = line.text.strip_prefix('-').unwrap().trim_start();
        let line_no = line.no;
        if rest.is_empty() {
            *pos += 1;
            if *pos >= lines.len() || lines[*pos].indent <= indent {
                items.push(Value::Null);
            } else {
                let child_indent = lines[*pos].indent;
                items.push(parse_node(lines, pos, child_indent)?);
            }
        } else if let Some((k, v)) = split_key_value(rest) {
            // Inline map item `- id: chief` followed by indented sibling keys.
            let mut map = Map::new();
            let key = parse_key(k, line_no)?;
            *pos += 1;
            let item_indent = indent + 2;
            let first_val = resolve_value_after_colon(v, lines, pos, item_indent, line_no)?;
            map.insert(key, first_val);
            if *pos < lines.len() && lines[*pos].indent > indent {
                let extra_indent = lines[*pos].indent;
                let extra = parse_mapping_into(lines, pos, extra_indent)?;
                for (ek, ev) in extra {
                    map.insert(ek, ev);
                }
            }
            items.push(Value::Object(map));
        } else {
            items.push(parse_scalar_or_flow(rest, line_no)?);
            *pos += 1;
        }
    }
    Ok(Value::Array(items))
}

fn parse_mapping(lines: &[Line<'_>], pos: &mut usize, indent: usize) -> Result<Value> {
    Ok(Value::Object(parse_mapping_into(lines, pos, indent)?))
}

fn parse_mapping_into(
    lines: &[Line<'_>],
    pos: &mut usize,
    indent: usize,
) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    while *pos < lines.len() {
        let line = &lines[*pos];
        if line.indent < indent {
            break;
        }
        ensure!(
            line.indent == indent,
            "line {}: unexpected mapping indentation (expected {indent}, got {})",
            line.no,
            line.indent
        );
        if is_seq_item(line.text) {
            break;
        }
        let Some((raw_k, raw_v)) = split_key_value(line.text) else {
            bail!(
                "line {}: expected `key: value`, got `{}`",
                line.no,
                line.text
            );
        };
        let line_no = line.no;
        let key = parse_key(raw_k, line_no)?;
        ensure!(
            !map.contains_key(&key),
            "line {line_no}: duplicate key `{key}`"
        );
        *pos += 1;
        let val = resolve_value_after_colon(raw_v, lines, pos, indent, line_no)?;
        map.insert(key, val);
    }
    Ok(map)
}

fn resolve_value_after_colon(
    raw_v: &str,
    lines: &[Line<'_>],
    pos: &mut usize,
    parent_indent: usize,
    line_no: usize,
) -> Result<Value> {
    let v = raw_v.trim();
    if v == "|" || v == ">" {
        let fold = v == ">";
        let mut chunks = Vec::new();
        if *pos < lines.len() && lines[*pos].indent > parent_indent {
            let block_indent = lines[*pos].indent;
            while *pos < lines.len() && lines[*pos].indent >= block_indent {
                chunks.push(lines[*pos].text);
                *pos += 1;
            }
        }
        let sep = if fold { " " } else { "\n" };
        return Ok(Value::String(chunks.join(sep)));
    }
    if v.is_empty() {
        if *pos < lines.len() && lines[*pos].indent > parent_indent {
            let child_indent = lines[*pos].indent;
            return parse_node(lines, pos, child_indent);
        }
        return Ok(Value::Null);
    }
    parse_scalar_or_flow(v, line_no)
}

fn split_key_value(text: &str) -> Option<(&str, &str)> {
    let mut in_single = false;
    let mut in_double = false;
    let mut bracket_depth = 0usize;
    let mut prev_escape = false;
    for (idx, ch) in text.char_indices() {
        if in_double {
            if prev_escape {
                prev_escape = false;
            } else if ch == '\\' {
                prev_escape = true;
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        match ch {
            '"' => in_double = true,
            '\'' => in_single = true,
            '[' | '{' => bracket_depth += 1,
            ']' | '}' => bracket_depth = bracket_depth.saturating_sub(1),
            ':' if bracket_depth == 0 => {
                let after = &text[idx + 1..];
                if after.is_empty() || after.starts_with(' ') {
                    return Some((text[..idx].trim(), after.trim_start()));
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_key(raw: &str, line_no: usize) -> Result<String> {
    let val = parse_scalar_or_flow(raw, line_no)?;
    match val {
        Value::String(s) if !s.is_empty() => Ok(s),
        _ => bail!("line {line_no}: mapping key must be a non-empty string"),
    }
}

fn parse_scalar_or_flow(raw: &str, line_no: usize) -> Result<Value> {
    let s = raw.trim();
    if s.starts_with('[') && s.ends_with(']') {
        return parse_flow_seq(&s[1..s.len() - 1], line_no);
    }
    if s.starts_with('{') && s.ends_with('}') {
        return parse_flow_map(&s[1..s.len() - 1], line_no);
    }
    if s.starts_with('"') {
        return serde_json::from_str::<String>(s)
            .map(Value::String)
            .with_context(|| format!("line {line_no}: invalid double-quoted string"));
    }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Ok(Value::String(s[1..s.len() - 1].replace("''", "'")));
    }
    match s {
        "null" | "Null" | "NULL" | "~" => Ok(Value::Null),
        "true" | "True" | "TRUE" => Ok(Value::Bool(true)),
        "false" | "False" | "FALSE" => Ok(Value::Bool(false)),
        _ => {
            if let Ok(int_val) = s.parse::<i64>() {
                return Ok(Value::Number(Number::from(int_val)));
            }
            if s.contains('.') {
                if let Ok(float_val) = s.parse::<f64>() {
                    if let Some(num) = Number::from_f64(float_val) {
                        return Ok(Value::Number(num));
                    }
                }
            }
            Ok(Value::String(s.to_owned()))
        }
    }
}

fn split_flow_items(inner: &str) -> Vec<&str> {
    if inner.trim().is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut prev_escape = false;
    let mut depth = 0usize;
    for (idx, ch) in inner.char_indices() {
        if in_double {
            if prev_escape {
                prev_escape = false;
            } else if ch == '\\' {
                prev_escape = true;
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        match ch {
            '"' => in_double = true,
            '\'' => in_single = true,
            '[' | '{' => depth += 1,
            ']' | '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(inner[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    let tail = inner[start..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    out
}

fn parse_flow_seq(inner: &str, line_no: usize) -> Result<Value> {
    let mut arr = Vec::new();
    for item in split_flow_items(inner) {
        arr.push(parse_scalar_or_flow(item, line_no)?);
    }
    Ok(Value::Array(arr))
}

fn parse_flow_map(inner: &str, line_no: usize) -> Result<Value> {
    let mut map = Map::new();
    for item in split_flow_items(inner) {
        let Some((k, v)) = split_key_value(item) else {
            bail!("line {line_no}: invalid flow map entry `{item}`");
        };
        map.insert(parse_key(k, line_no)?, parse_scalar_or_flow(v, line_no)?);
    }
    Ok(Value::Object(map))
}
