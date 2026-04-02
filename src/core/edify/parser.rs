//! Edify script parser — produces an AST from the OTA updater-script language.

use anyhow::{bail, Context, Result};

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
                let e = self.parse_expr()?;
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

pub fn parse_edify(script: &str) -> Result<Expr> {
    let mut parser = Parser::new(script)?;
    parser.parse_script()
}
