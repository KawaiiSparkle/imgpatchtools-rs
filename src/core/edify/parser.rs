//! Edify script parser — produces an AST from the OTA updater-script language.

use anyhow::{Context, Result, bail};

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOperator {
    Or,
    And,
    Eq,
    Add,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    StringLiteral(String),
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    Sequence(Vec<Expr>),
    If {
        condition: Box<Expr>,
        then: Box<Expr>,
        else_: Option<Box<Expr>>,
    },
    BinaryOp {
        op: BinaryOperator,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Str(String),
    LParen,
    RParen,
    Comma,
    Semi,
    If,
    Then,
    Else,
    Endif,
    OrOr,
    AndAnd,
    EqEq,
    Plus,
    Eof,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn skip_whitespace_and_comments(&mut self) {
        while self.pos < self.input.len() {
            let b = self.input[self.pos];
            if b.is_ascii_whitespace() {
                self.pos += 1;
                continue;
            }
            if b == b'#' {
                while self.pos < self.input.len() && self.input[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments();
        let ch = match self.input.get(self.pos) {
            Some(&c) => c,
            None => return Ok(Token::Eof),
        };

        match ch {
            b'(' => {
                self.pos += 1;
                Ok(Token::LParen)
            }
            b')' => {
                self.pos += 1;
                Ok(Token::RParen)
            }
            b',' => {
                self.pos += 1;
                Ok(Token::Comma)
            }
            b';' => {
                self.pos += 1;
                Ok(Token::Semi)
            }
            b'"' => self.read_quoted_string(),
            b'|' if self.input.get(self.pos + 1) == Some(&b'|') => {
                self.pos += 2;
                Ok(Token::OrOr)
            }
            b'&' if self.input.get(self.pos + 1) == Some(&b'&') => {
                self.pos += 2;
                Ok(Token::AndAnd)
            }
            b'=' if self.input.get(self.pos + 1) == Some(&b'=') => {
                self.pos += 2;
                Ok(Token::EqEq)
            }
            b'+' => {
                self.pos += 1;
                Ok(Token::Plus)
            }
            _ => self.read_bare_word(),
        }
    }

    fn read_quoted_string(&mut self) -> Result<Token> {
        self.pos += 1; // skip "
        let mut s = String::new();
        while self.pos < self.input.len() {
            match self.input[self.pos] {
                b'"' => {
                    self.pos += 1;
                    return Ok(Token::Str(s));
                }
                b'\\' => {
                    self.pos += 1;
                    if self.pos < self.input.len() {
                        s.push(self.input[self.pos] as char);
                        self.pos += 1;
                    }
                }
                c => {
                    s.push(c as char);
                    self.pos += 1;
                }
            }
        }
        bail!("unterminated string")
    }

    fn read_bare_word(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input[self.pos];
            if c.is_ascii_whitespace() || matches!(c, b'(' | b')' | b',' | b';' | b'"' | b'#') {
                break;
            }
            self.pos += 1;
        }
        let word = std::str::from_utf8(&self.input[start..self.pos])
            .context("invalid utf8")?
            .to_string();

        match word.as_str() {
            "if" => Ok(Token::If),
            "then" => Ok(Token::Then),
            "else" => Ok(Token::Else),
            "endif" => Ok(Token::Endif),
            _ => Ok(Token::Str(word)),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Result<Self> {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token()?;
        Ok(Self { lexer, current })
    }

    fn bump(&mut self) -> Result<Token> {
        let old = std::mem::replace(&mut self.current, Token::Eof);
        self.current = self.lexer.next_token()?;
        Ok(old)
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        if self.current != expected {
            bail!("expected {:?}, got {:?}", expected, self.current);
        }
        self.bump()?;
        Ok(())
    }

    pub fn parse_script(&mut self) -> Result<Expr> {
        self.parse_statements(&[])
    }

    fn parse_statements(&mut self, terminators: &[Token]) -> Result<Expr> {
        let mut exprs = Vec::new();
        while self.current != Token::Eof && !terminators.contains(&self.current) {
            exprs.push(self.parse_expr()?);
            if self.current == Token::Semi {
                self.bump()?;
            }
        }
        if exprs.len() == 1 {
            Ok(exprs.remove(0))
        } else if exprs.is_empty() {
            Ok(Expr::StringLiteral(String::new()))
        } else {
            Ok(Expr::Sequence(exprs))
        }
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut lhs = self.parse_and()?;
        while self.current == Token::OrOr {
            self.bump()?;
            let rhs = self.parse_and()?;
            lhs = Expr::BinaryOp {
                op: BinaryOperator::Or,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut lhs = self.parse_equality()?;
        while self.current == Token::AndAnd {
            self.bump()?;
            let rhs = self.parse_equality()?;
            lhs = Expr::BinaryOp {
                op: BinaryOperator::And,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_equality(&mut self) -> Result<Expr> {
        let mut lhs = self.parse_add()?;
        while self.current == Token::EqEq {
            self.bump()?;
            let rhs = self.parse_add()?;
            lhs = Expr::BinaryOp {
                op: BinaryOperator::Eq,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_add(&mut self) -> Result<Expr> {
        let mut lhs = self.parse_primary()?;
        while self.current == Token::Plus {
            self.bump()?;
            let rhs = self.parse_primary()?;
            lhs = Expr::BinaryOp {
                op: BinaryOperator::Add,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match &self.current {
            Token::If => self.parse_if(),
            Token::LParen => {
                self.bump()?;
                // Parse statements (supports sequences like "(stmt1; stmt2)")
                let e = self.parse_statements(&[Token::RParen])?;
                self.expect(Token::RParen)?;
                Ok(e)
            }
            Token::Str(_) => {
                let name = match self.bump()? {
                    Token::Str(s) => s,
                    _ => unreachable!(),
                };
                if self.current == Token::LParen {
                    self.bump()?; // consume '('
                    let mut args = vec![];
                    if self.current != Token::RParen {
                        args.push(self.parse_expr()?);
                        while self.current == Token::Comma {
                            self.bump()?;
                            args.push(self.parse_expr()?);
                        }
                    }
                    self.expect(Token::RParen)?;
                    Ok(Expr::FunctionCall { name, args })
                } else {
                    Ok(Expr::StringLiteral(name))
                }
            }
            other => bail!("unexpected token: {:?}", other),
        }
    }

    fn parse_if(&mut self) -> Result<Expr> {
        self.expect(Token::If)?;
        let condition = Box::new(self.parse_expr()?);
        self.expect(Token::Then)?;

        // 核心修复：then 的内容可以是一个语句序列
        let then = Box::new(self.parse_statements(&[Token::Else, Token::Endif])?);

        let else_ = if self.current == Token::Else {
            self.bump()?;
            // 核心修复：else 的内容也可以是一个语句序列
            Some(Box::new(self.parse_statements(&[Token::Endif])?))
        } else {
            None
        };

        self.expect(Token::Endif)?;
        Ok(Expr::If {
            condition,
            then,
            else_,
        })
    }
}

/// Parse an edify script, automatically skipping UTF-8 BOM if present.
pub fn parse_edify(script: &str) -> Result<Expr> {
    // Skip UTF-8 BOM (U+FEFF) if present.
    let script = script.strip_prefix('\u{FEFF}').unwrap_or(script);
    let mut parser = Parser::new(script)?;
    parser.parse_script()
}

/// Find update-script in current directory or META-INF/com/google/android/
fn find_update_script() -> Result<std::path::PathBuf> {
    let current_dir = std::env::current_dir()?;

    // Try current directory first
    let script_in_root = current_dir.join("update-script");
    if script_in_root.exists() {
        return Ok(script_in_root);
    }

    // Try META-INF/com/google/android/
    let script_in_meta = current_dir
        .join("META-INF")
        .join("com")
        .join("google")
        .join("android")
        .join("update-script");
    if script_in_meta.exists() {
        return Ok(script_in_meta);
    }

    anyhow::bail!("update-script not found in current directory or META-INF/com/google/android/")
}

/// RangeSha1Info holds both the ranges and expected SHA1 from update-script
#[derive(Debug, Clone)]
pub struct RangeSha1Info {
    pub ranges: String,
    pub expected_sha1: Option<String>,
}

/// Read range_sha1 ranges for a partition from update-script.
/// Searches for: range_sha1("/dev/.../partition", "ranges") == "expected_sha1"
pub fn read_range_sha1_from_script(partition_name: &str) -> Result<String> {
    let info = read_range_sha1_info_from_script(partition_name)?;
    Ok(info.ranges)
}

/// Read both ranges and expected SHA1 from update-script.
/// Searches for: range_sha1("/dev/.../partition", "ranges") == "expected_sha1"
pub fn read_range_sha1_info_from_script(partition_name: &str) -> Result<RangeSha1Info> {
    let script_path = find_update_script()?;
    let content = std::fs::read_to_string(&script_path)
        .with_context(|| format!("Failed to read {}", script_path.display()))?;

    // Parse the script to find range_sha1 calls for this partition
    let ast = parse_edify(&content)?;

    // Search for range_sha1 calls in the AST with expected SHA1
    if let Some(info) = extract_range_sha1_info_from_expr(&ast, partition_name) {
        return Ok(info);
    }

    // Fallback: use regex-like search if AST parsing doesn't find it
    find_range_sha1_info_in_text(&content, partition_name)
}

/// Extract range_sha1 info (ranges + expected SHA1) from AST expression
fn extract_range_sha1_info_from_expr(expr: &Expr, partition_name: &str) -> Option<RangeSha1Info> {
    match expr {
        // Handle: range_sha1("path", "ranges") == "expected_sha1" or "expected_sha1" == range_sha1("path", "ranges")
        Expr::BinaryOp {
            op: BinaryOperator::Eq,
            lhs,
            rhs,
        } => {
            // Check if lhs is range_sha1 call
            if let Expr::FunctionCall { name, args } = lhs.as_ref()
                && name == "range_sha1" && args.len() >= 2
                    && let (Expr::StringLiteral(path), Expr::StringLiteral(ranges)) =
                        (&args[0], &args[1])
                        && (path.contains(partition_name)
                            || path.contains(&partition_name.replace("_", "/")))
                        {
                            // Get expected SHA1 from rhs
                            let expected_sha1 = match rhs.as_ref() {
                                Expr::StringLiteral(s) => Some(s.clone()),
                                _ => None,
                            };
                            return Some(RangeSha1Info {
                                ranges: ranges.clone(),
                                expected_sha1,
                            });
                        }
            // Check if rhs is range_sha1 call (reverse order)
            if let Expr::FunctionCall { name, args } = rhs.as_ref()
                && name == "range_sha1" && args.len() >= 2
                    && let (Expr::StringLiteral(path), Expr::StringLiteral(ranges)) =
                        (&args[0], &args[1])
                        && (path.contains(partition_name)
                            || path.contains(&partition_name.replace("_", "/")))
                        {
                            let expected_sha1 = match lhs.as_ref() {
                                Expr::StringLiteral(s) => Some(s.clone()),
                                _ => None,
                            };
                            return Some(RangeSha1Info {
                                ranges: ranges.clone(),
                                expected_sha1,
                            });
                        }
            // Try searching in both sides recursively
            extract_range_sha1_info_from_expr(lhs, partition_name)
                .or_else(|| extract_range_sha1_info_from_expr(rhs, partition_name))
        }
        // Handle standalone range_sha1 call (without comparison)
        Expr::FunctionCall { name, args } if name == "range_sha1" => {
            if args.len() >= 2
                && let (Expr::StringLiteral(path), Expr::StringLiteral(ranges)) =
                    (&args[0], &args[1])
                    && (path.contains(partition_name)
                        || path.contains(&partition_name.replace("_", "/")))
                    {
                        return Some(RangeSha1Info {
                            ranges: ranges.clone(),
                            expected_sha1: None,
                        });
                    }
            None
        }
        Expr::Sequence(exprs) => {
            for e in exprs {
                if let Some(info) = extract_range_sha1_info_from_expr(e, partition_name) {
                    return Some(info);
                }
            }
            None
        }
        Expr::If {
            condition: _,
            then,
            else_,
        } => {
            if let Some(info) = extract_range_sha1_info_from_expr(then, partition_name) {
                return Some(info);
            }
            if let Some(else_expr) = else_
                && let Some(info) = extract_range_sha1_info_from_expr(else_expr, partition_name) {
                    return Some(info);
                }
            None
        }
        Expr::BinaryOp { op: _, lhs, rhs } => {
            extract_range_sha1_info_from_expr(lhs, partition_name)
                .or_else(|| extract_range_sha1_info_from_expr(rhs, partition_name))
        }
        _ => None,
    }
}

/// Fallback: find range_sha1 info (ranges + expected SHA1) in text using simple pattern matching
fn find_range_sha1_info_in_text(content: &str, partition_name: &str) -> Result<RangeSha1Info> {
    // Try various partition name formats
    let patterns = vec![partition_name.to_string(), partition_name.replace("_", "/")];

    for line in content.lines() {
        let line = line.trim();
        if line.contains("range_sha1") {
            for pattern in &patterns {
                if line.contains(pattern) {
                    // Try to extract ranges and expected SHA1
                    // Pattern: range_sha1("path", "ranges") == "sha1" or range_sha1("path", "ranges") == "sha1"
                    if let Some(start) = line.find("range_sha1(")
                        && let Some(args_start) = line[start..].find('"') {
                            let after_first = &line[start + args_start + 1..];
                            if let Some(second_quote) = after_first.find('"') {
                                let after_path = &after_first[second_quote + 1..];
                                if let Some(comma) = after_path.find(',') {
                                    let after_comma = &after_path[comma + 1..].trim();
                                    if after_comma.starts_with('"') {
                                        let ranges_start = 1;
                                        if let Some(ranges_end) =
                                            after_comma[ranges_start..].find('"')
                                        {
                                            let ranges = after_comma
                                                [ranges_start..ranges_start + ranges_end]
                                                .to_string();

                                            // Try to find expected SHA1 after the closing paren
                                            let after_call =
                                                &after_comma[ranges_start + ranges_end + 1..];
                                            let expected_sha1 = extract_expected_sha1(after_call);

                                            return Ok(RangeSha1Info {
                                                ranges,
                                                expected_sha1,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                }
            }
        }
    }

    anyhow::bail!(
        "range_sha1 ranges not found for partition: {}",
        partition_name
    )
}

/// Extract expected SHA1 from the text after range_sha1() call
/// Handles: ) == "sha1" or )=="sha1" or ) == "sha1" ||
fn extract_expected_sha1(text: &str) -> Option<String> {
    // Look for == "sha1" pattern
    if let Some(eq_pos) = text.find("==") {
        let after_eq = &text[eq_pos + 2..].trim();
        if after_eq.starts_with('"') {
            let sha1_start = 1;
            if let Some(sha1_end) = after_eq[sha1_start..].find('"') {
                let sha1 = &after_eq[sha1_start..sha1_start + sha1_end];
                // Validate it looks like a SHA1 (40 hex chars)
                if sha1.len() == 40 && sha1.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(sha1.to_string());
                }
            }
        }
    }
    None
}
