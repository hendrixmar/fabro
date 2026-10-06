use crate::Error;
use crate::graph::Node;

/// A parsed stylesheet selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    /// `*` -- matches all nodes, specificity 0.
    Universal,
    /// Bare word -- matches nodes by shape name, specificity 1.
    Shape(String),
    /// `.classname` -- matches nodes with that class, specificity 2.
    Class(String),
    /// `#nodeid` -- matches a specific node, specificity 3.
    Id(String),
}

impl Selector {
    #[must_use]
    pub const fn specificity(&self) -> u8 {
        match self {
            Self::Universal => 0,
            Self::Shape(_) => 1,
            Self::Class(_) => 2,
            Self::Id(_) => 3,
        }
    }

    /// Returns whether this selector matches the given graph node.
    #[must_use]
    pub fn matches_node(&self, node_id: &str, node: &Node) -> bool {
        match self {
            Self::Universal => true,
            Self::Shape(shape) => node.shape() == shape,
            Self::Class(class) => node.classes.contains(class),
            Self::Id(id) => node_id == id,
        }
    }
}

/// A single CSS-like declaration: `property: value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    pub property: String,
    pub value:    String,
}

/// A stylesheet rule: selector + declarations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub selector:     Selector,
    pub declarations: Vec<Declaration>,
}

/// A parsed stylesheet containing multiple rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stylesheet {
    pub rules: Vec<Rule>,
}

/// Parse a stylesheet string into a `Stylesheet`.
///
/// # Errors
///
/// Returns an error if the input contains invalid stylesheet syntax.
pub fn parse_stylesheet(input: &str) -> Result<Stylesheet, Error> {
    let input = strip_css_comments(input)?;
    let input = input.trim();
    if input.is_empty() {
        return Ok(Stylesheet { rules: Vec::new() });
    }

    let mut rules = Vec::new();
    let mut remaining = input;

    while !remaining.trim().is_empty() {
        remaining = remaining.trim();

        let selector = parse_selector(&mut remaining)?;
        if !remaining.starts_with('{') {
            return Err(Error::Stylesheet(format!(
                "expected '{{' after selector, got: {:?}",
                excerpt(remaining)
            )));
        }
        remaining = remaining[1..].trim();

        let declarations = parse_declarations(&mut remaining)?;
        remaining = remaining[1..].trim(); // skip '}'

        rules.push(Rule {
            selector,
            declarations,
        });
    }

    Ok(Stylesheet { rules })
}

fn strip_css_comments(input: &str) -> Result<String, Error> {
    let mut output = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(start) = remaining.find("/*") {
        let body = &remaining[start + 2..];
        let Some(end) = body.find("*/") else {
            return Err(Error::Stylesheet(format!(
                "unterminated CSS comment: {:?}",
                excerpt(&remaining[start..])
            )));
        };

        output.push_str(&remaining[..start]);
        // CSS comments do not join the tokens on either side. Preserve
        // that boundary for this parser with one whitespace character.
        output.push(' ');
        remaining = &body[end + 2..];
    }

    output.push_str(remaining);
    Ok(output)
}

/// A short excerpt of `input` for error messages, cut on a character boundary.
fn excerpt(input: &str) -> String {
    input.chars().take(20).collect()
}

fn parse_selector(remaining: &mut &str) -> Result<Selector, Error> {
    if remaining.starts_with('*') {
        *remaining = remaining[1..].trim();
        Ok(Selector::Universal)
    } else if remaining.starts_with('#') {
        *remaining = remaining[1..].trim();
        let end = remaining
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .unwrap_or(remaining.len());
        if end == 0 {
            return Err(Error::Stylesheet("expected identifier after '#'".into()));
        }
        let id = remaining[..end].to_string();
        *remaining = remaining[end..].trim();
        Ok(Selector::Id(id))
    } else if remaining.starts_with('.') {
        *remaining = remaining[1..].trim();
        let end = remaining
            .find(|c: char| !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-')
            .unwrap_or(remaining.len());
        if end == 0 {
            return Err(Error::Stylesheet("expected class name after '.'".into()));
        }
        let class = remaining[..end].to_string();
        *remaining = remaining[end..].trim();
        Ok(Selector::Class(class))
    } else {
        // Bare word: shape selector
        let end = remaining
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .unwrap_or(remaining.len());
        if end == 0 {
            return Err(Error::Stylesheet(format!(
                "expected selector ('*', '#id', '.class', or shape name), got: {:?}",
                excerpt(remaining)
            )));
        }
        let shape = remaining[..end].to_string();
        *remaining = remaining[end..].trim();
        Ok(Selector::Shape(shape))
    }
}

fn parse_declarations(remaining: &mut &str) -> Result<Vec<Declaration>, Error> {
    let mut declarations = Vec::new();
    while !remaining.starts_with('}') {
        if remaining.is_empty() {
            return Err(Error::Stylesheet(
                "unexpected end of stylesheet, expected '}'".into(),
            ));
        }
        if remaining.starts_with(';') {
            *remaining = remaining[1..].trim();
            continue;
        }

        let prop_end = remaining
            .find(|c: char| c == ':' || c.is_whitespace())
            .unwrap_or(remaining.len());
        let property = remaining[..prop_end].to_string();
        *remaining = remaining[prop_end..].trim();

        if !remaining.starts_with(':') {
            return Err(Error::Stylesheet(format!(
                "expected ':' after property name '{property}'"
            )));
        }
        *remaining = remaining[1..].trim();

        let val_end = remaining.find([';', '}']).unwrap_or(remaining.len());
        let value = remaining[..val_end].trim().to_string();
        *remaining = remaining[val_end..].trim();

        if value.is_empty() {
            return Err(Error::Stylesheet(format!(
                "empty value for property '{property}'"
            )));
        }

        declarations.push(Declaration { property, value });

        if remaining.starts_with(';') {
            *remaining = remaining[1..].trim();
        }
    }
    Ok(declarations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_stylesheet() {
        let ss = parse_stylesheet("").unwrap();
        assert!(ss.rules.is_empty());
    }

    #[test]
    fn parse_universal_rule() {
        let ss = parse_stylesheet("* { model: claude-sonnet-4-5; provider: anthropic; }").unwrap();
        assert_eq!(ss.rules.len(), 1);
        assert_eq!(ss.rules[0].selector, Selector::Universal);
        assert_eq!(ss.rules[0].declarations.len(), 2);
        assert_eq!(ss.rules[0].declarations[0].property, "model");
        assert_eq!(ss.rules[0].declarations[0].value, "claude-sonnet-4-5");
    }

    #[test]
    fn parse_class_rule() {
        let ss = parse_stylesheet(".code { model: claude-opus-4-6; }").unwrap();
        assert_eq!(ss.rules[0].selector, Selector::Class("code".into()));
    }

    #[test]
    fn parse_id_rule() {
        let ss = parse_stylesheet("#critical_review { model: gpt-5.2; reasoning_effort: high; }")
            .unwrap();
        assert_eq!(ss.rules[0].selector, Selector::Id("critical_review".into()));
        assert_eq!(ss.rules[0].declarations.len(), 2);
    }

    #[test]
    fn parse_multiple_rules() {
        let input = r"
            * { model: claude-sonnet-4-5; provider: anthropic; }
            .code { model: claude-opus-4-6; provider: anthropic; }
            #critical_review { model: gpt-5.2; provider: openai; reasoning_effort: high; }
        ";
        let ss = parse_stylesheet(input).unwrap();
        assert_eq!(ss.rules.len(), 3);
    }

    #[test]
    fn parse_css_comments_between_tokens() {
        let input = r"
            /* Defaults apply to every node. */
            */* selector */{/* before property */
                model/* before colon */:/* before value */claude-sonnet-4-5/* after value */;
            /* before closing brace */}
            /* Use Opus for coding nodes. */
            .code { model: claude-opus-4-6; }
        ";

        let ss = parse_stylesheet(input).unwrap();
        assert_eq!(ss.rules.len(), 2);
        assert_eq!(ss.rules[0].selector, Selector::Universal);
        assert_eq!(ss.rules[0].declarations[0].property, "model");
        assert_eq!(ss.rules[0].declarations[0].value, "claude-sonnet-4-5");
        assert_eq!(ss.rules[1].selector, Selector::Class("code".into()));
    }

    #[test]
    fn parse_comment_only_stylesheet() {
        let ss = parse_stylesheet("/* no rules */").unwrap();
        assert!(ss.rules.is_empty());
    }

    #[test]
    fn comments_do_not_join_tokens() {
        let result = parse_stylesheet("* { mo/**/del: sonnet; }");
        assert!(result.is_err());
    }

    #[test]
    fn comments_close_at_first_terminator() {
        let ss = parse_stylesheet("/* outer /* inner */ * { model: sonnet; }").unwrap();
        assert_eq!(ss.rules.len(), 1);
        assert_eq!(ss.rules[0].selector, Selector::Universal);
    }

    #[test]
    fn parse_error_unterminated_comment() {
        let error = parse_stylesheet("/* no terminator").unwrap_err();
        assert_eq!(
            error.to_string(),
            r#"Stylesheet error: unterminated CSS comment: "/* no terminator""#
        );
    }

    #[test]
    fn parse_error_line_comment() {
        let error = parse_stylesheet("// not a CSS comment\n* { model: sonnet; }").unwrap_err();
        assert!(
            error.to_string().contains("expected selector"),
            "`//` should be reported as a bad selector, got: {error}"
        );
    }

    #[test]
    fn parse_error_excerpt_splits_on_character_boundary() {
        // A multi-byte character straddling the excerpt cutoff must not panic.
        let error = parse_stylesheet("/* ünterminated cömment, well over 20 bytes").unwrap_err();
        assert_eq!(
            error.to_string(),
            r#"Stylesheet error: unterminated CSS comment: "/* ünterminated cömm""#
        );
    }

    #[test]
    fn parse_error_missing_brace() {
        let result = parse_stylesheet("* model: test; }");
        assert!(result.is_err());
    }

    #[test]
    fn parse_error_missing_selector() {
        let result = parse_stylesheet("{ model: test; }");
        assert!(result.is_err());
    }

    #[test]
    fn parse_shape_selector() {
        let ss = parse_stylesheet("box { model: opus; }").unwrap();
        assert_eq!(ss.rules.len(), 1);
        assert_eq!(ss.rules[0].selector, Selector::Shape("box".into()));
        assert_eq!(ss.rules[0].declarations[0].value, "opus");
    }

    #[test]
    fn selector_specificity_values() {
        assert_eq!(Selector::Universal.specificity(), 0);
        assert_eq!(Selector::Shape("box".into()).specificity(), 1);
        assert_eq!(Selector::Class("x".into()).specificity(), 2);
        assert_eq!(Selector::Id("x".into()).specificity(), 3);
    }
}
