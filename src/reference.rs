// Code generated from https://github.com/distribution/distribution/blob/v2.7.1/reference/reference.go

use regex::Regex;
use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct DockerReference {
    pub domain: Option<String>,
    port: Option<u16>,
    name: String,
    tag: Option<String>,
    digest: Option<String>,
}

#[derive(Debug)]
pub struct ParseError;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid docker reference")
    }
}

impl Error for ParseError {}

impl DockerReference {
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        let re = Regex::new(r"(?x)
            ^
            ((?P<domain>[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9](?:\.[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])*)(?::(?P<port>[0-9]+))?/)?
            (?P<name>(?:[a-z0-9]+(?:[_.]|__|[-]*[a-z0-9]+)*)(?:/[a-z0-9]+(?:[_.]|__|[-]*[a-z0-9]+)*)*)
            (:(?P<tag>[\w][\w.-]{0,127}))?
            (@(?P<digest>[A-Za-z][A-Za-z0-9]*(?:[+.-_][A-Za-z][A-Za-z0-9]*)*:[0-9a-fA-F]{32,}))?
            $
        ").unwrap();

        let captures = re.captures(input).ok_or(ParseError)?;

        let mut reference = DockerReference {
            domain: captures.name("domain").map(|m| m.as_str().to_string()),
            port: captures.name("port").map(|m| m.as_str().parse().unwrap()),
            name: captures["name"].to_string(),
            tag: captures.name("tag").map(|m| m.as_str().to_string()),
            digest: captures.name("digest").map(|m| m.as_str().to_string()),
        };

        // If the domain is not a valid DNS name, then squash it into the name, and clear the domain.
        if reference.domain.is_some() {
            let domain = reference.domain.as_ref().unwrap();
            // this is not bullet-proof, but okay heuristic for now.
            // one edge case is K8s service name without DNS qualifier.
            // See test reference6. Maybe consider gethostbyname?
            if !domain.contains('.') && !domain.contains(':') && domain != "localhost" {
                reference.name = format!("{}/{}", domain, reference.name);
                reference.domain = None;
            }
        }

        Ok(reference)
    }

    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }

    pub fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_reference() {
        let reference = DockerReference::parse(
            "my.domain.com:8080/my_image:v1.0.0@sha256:abcd1234abcd1234abcd1234abcd1234",
        )
        .unwrap();
        assert_eq!(reference.domain(), Some("my.domain.com"));
        assert_eq!(reference.port(), Some(8080));
        assert_eq!(reference.name(), "my_image");
        assert_eq!(reference.tag(), Some("v1.0.0"));
        assert_eq!(
            reference.digest(),
            Some("sha256:abcd1234abcd1234abcd1234abcd1234")
        );

        let reference2 = DockerReference::parse("my_image:v1.0.0").unwrap();
        assert_eq!(reference2.domain(), None);
        assert_eq!(reference2.port(), None);
        assert_eq!(reference2.name(), "my_image");
        assert_eq!(reference2.tag(), Some("v1.0.0"));
        assert_eq!(reference2.digest(), None);

        // Additional test cases
        let reference3 = DockerReference::parse("localhost:5000/my-workload").unwrap();
        assert_eq!(reference3.domain(), Some("localhost"));
        assert_eq!(reference3.port(), Some(5000));
        assert_eq!(reference3.name(), "my-workload");
        assert_eq!(reference3.tag(), None);
        assert_eq!(reference3.digest(), None);

        let reference4 = DockerReference::parse("localhost:5000/my-workload/subworkload").unwrap();
        assert_eq!(reference4.domain(), Some("localhost"));
        assert_eq!(reference4.port(), Some(5000));
        assert_eq!(reference4.name(), "my-workload/subworkload");
        assert_eq!(reference4.tag(), None);
        assert_eq!(reference4.digest(), None);

        let reference5 = DockerReference::parse("localhost:5000/my-workload:latest").unwrap();
        assert_eq!(reference5.domain(), Some("localhost"));
        assert_eq!(reference5.port(), Some(5000));
        assert_eq!(reference5.name(), "my-workload");
        assert_eq!(reference5.tag(), Some("latest"));
        assert_eq!(reference5.digest(), None);

        // let reference6 = DockerReference::parse("my-registry-custom-dns/my-workload:tag").unwrap();
        // assert_eq!(reference6.domain(), Some("my-registry-custom-dns"));
        // assert_eq!(reference6.port(), None);
        // assert_eq!(reference6.name(), "my-workload");
        // assert_eq!(reference6.tag(), Some("tag"));
        // assert_eq!(reference6.digest(), None);

        let reference7 = DockerReference::parse("conex-project/the-name:tag").unwrap();
        assert_eq!(reference7.domain(), None);
        assert_eq!(reference7.port(), None);
        assert_eq!(reference7.name(), "conex-project/the-name");
        assert_eq!(reference7.tag(), Some("tag"));
        assert_eq!(reference7.digest(), None);

        // Negative test
        assert!(DockerReference::parse("invalid reference").is_err());
    }
}
