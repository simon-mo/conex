use std::{collections::HashMap, path::PathBuf};

use reqwest::{Method, Request};

use crate::reference::DockerReference;

fn parse_www_authenticate_fields(input: &str) -> HashMap<String, String> {
    // written by gpt4: https://chat.openai.com/share/208be7c4-5e87-4949-bafd-addcff76d34f

    let mut map = HashMap::new();
    let mut start = 0;
    let mut in_quotes = false;

    for (i, c) in input.chars().enumerate() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                let (key, value) = split_segment(&input[start..i]);
                map.insert(key, value.trim_matches('"').to_string());
                start = i + 1;
            }
            _ => {}
        }
    }

    fn split_segment(segment: &str) -> (String, &str) {
        let mut split = segment.splitn(2, '=');
        (
            split.next().unwrap_or("").to_string(),
            split.next().unwrap_or(""),
        )
    }

    // Handle the last segment
    let (key, value) = split_segment(&input[start..]);
    map.insert(key, value.trim_matches('"').to_string());

    map
}

#[derive(Clone, Debug)]
pub struct RepoInfo {
    pub raw_tag: String, // e.g. localhost:5000/my-repo-name-image-name:latest

    pub reference: DockerReference,

    registry_host_url: String,

    pub auth_token: Option<String>,
}

impl RepoInfo {
    pub async fn from_string(source_image: String) -> RepoInfo {
        let mut reference = DockerReference::parse(&source_image).unwrap();

        if reference.digest().is_some() {
            unimplemented!("Digest in image reference is not supported yet.");
        }

        if reference.domain() == Some("docker.io") {
            reference.domain = None;
        }

        let protocol = {
            match reference.domain() {
                Some(domain) => {
                    if domain == "localhost" || domain.starts_with("127.") {
                        "http".to_string()
                    } else {
                        "https".to_string()
                    }
                }
                None => "https".to_string(), // default to docker hub
            }
        };

        let registry_host_url = match reference.domain() {
            Some(domain) => format!(
                "{}://{}{}",
                protocol,
                domain,
                reference
                    .port()
                    .map(|p| format!(":{}", p))
                    .unwrap_or("".to_string())
            ),
            None => {
                // TODO: We should also handle the case of explicit docker.io.
                "https://registry-1.docker.io".to_string()
            }
        };

        let mut auth_token = None;
        if protocol == "https" {
            // Authenticate and add token to auth_headers
            // Figure out the auth realm, service, and scope by issuing a test request
            // we should get a 401 back with following header
            // www-authenticate: Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:nvidia/pytorch:pull"
            let client = reqwest::Client::new();
            let test_resp = client
                .get(&format!(
                    "{}/v2/{}/manifests/{}",
                    registry_host_url,
                    reference.name(),
                    reference.tag().unwrap_or("latest")
                ))
                .send()
                .await
                .unwrap();
            assert!(
                test_resp.status() == 401,
                "expect status code to be Unauthorized (401) so we can perform proper authentication (got {})",
                test_resp.status()
            );
            let header = test_resp
                .headers()
                .get("www-authenticate")
                .unwrap()
                .to_str()
                .unwrap();
            assert!(header.starts_with("Bearer "));
            let header = header.trim_start_matches("Bearer").trim_start();
            let auth_request_data = parse_www_authenticate_fields(header);

            // https://username:password@auth.docker.io/token?service=registry.docker.io&scope=repository:simonmok/conex-workload:pull,push
            // TODO: make this fine-grained to only request the scope we need.
            let mut token_req = client.get(&auth_request_data["realm"]).query(&[(
                "service",
                auth_request_data
                    .get("service")
                    .unwrap_or(
                        &reference
                            .domain()
                            .unwrap_or("https://registry-1.docker.io")
                            .to_string(),
                    )
                    .to_owned(),
            )]);

            if reference.domain().is_none() {
                // docker hub
                let config = PathBuf::from("/home/ubuntu") // hard-coding this due to running under root.
                    .join(".docker")
                    .join("config.json");
                let config = std::fs::read_to_string(config).unwrap();
                let config: serde_json::Value = serde_json::from_str(&config).unwrap();
                let auth = config["auths"]["https://index.docker.io/v1/"]["auth"]
                    .as_str()
                    .unwrap();
                let auth = data_encoding::BASE64.decode(auth.as_bytes()).unwrap();
                let auth = String::from_utf8(auth).unwrap();
                let auth = auth.split(':').collect::<Vec<_>>();

                token_req = token_req.basic_auth(auth[0], Some(auth[1])).query(&[(
                    "scope",
                    format!("repository:{}:pull,push", reference.name()),
                )]);
            } else {
                // private registry
                token_req = token_req.query(&[(
                    "scope",
                    auth_request_data["scope"].clone(), // because the test reques tonly concerns pull, we sometimes need to push as well.
                                                        // format!("repository:{}:pull,push", reference.name()),
                )]);
            }

            let token_resp = token_req
                // .basic_auth(auth[0], Some(auth[1]))
                .send()
                .await
                .unwrap();
            assert!(
                token_resp.status() == 200,
                "status_code: {:?}, headers: {:?}",
                token_resp.status(),
                token_resp.headers()
            );
            let token_resp = token_resp.json::<serde_json::Value>().await.unwrap();
            let token = token_resp["token"].as_str().unwrap();
            auth_token.replace(format!("Bearer {}", token).to_string());
        }

        Self {
            raw_tag: source_image.to_string(),
            reference,
            registry_host_url,
            auth_token,
        }
    }

    pub fn upload_blob_request(&self) -> Request {
        let mut req = Request::new(
            Method::POST,
            url::Url::parse(
                format!(
                    "{}/v2/{}/blobs/uploads/",
                    self.registry_host_url,
                    self.reference.name()
                )
                .as_str(),
            )
            .unwrap(),
        );

        if let Some(v) = self.auth_token.as_ref() {
            req.headers_mut().insert(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(v.as_str()).unwrap(),
            );
        }

        req
    }

    pub fn upload_manifest_request(&self, tag: &str) -> Request {
        let mut req = Request::new(
            Method::PUT,
            url::Url::parse(
                format!(
                    "{}/v2/{}/manifests/{}",
                    self.registry_host_url,
                    self.reference.name(),
                    tag
                )
                .as_str(),
            )
            .unwrap(),
        );

        if let Some(v) = self.auth_token.as_ref() {
            req.headers_mut().insert(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(v.as_str()).unwrap(),
            );
        }

        req
    }

    pub fn get_manifest_request(&self) -> Request {
        let mut req = Request::new(
            Method::GET,
            url::Url::parse(
                format!(
                    "{}/v2/{}/manifests/{}",
                    self.registry_host_url,
                    self.reference.name(),
                    self.reference.tag().unwrap_or("latest")
                )
                .as_str(),
            )
            .unwrap(),
        );
        req.headers_mut().insert(
            http::header::ACCEPT,
            http::HeaderValue::from_str("application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json").unwrap(),
        );

        if let Some(v) = self.auth_token.as_ref() {
            req.headers_mut().insert(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(v.as_str()).unwrap(),
            );
        }

        req
    }

    pub fn get_config_request(&self, digest: &String) -> Request {
        let mut req = self.get_blob_request(digest);
        req.headers_mut().insert(
            http::header::ACCEPT,
            http::HeaderValue::from_str("application/vnd.oci.image.config.v1+json").unwrap(),
        );
        req
    }

    pub fn get_blob_request(&self, digest: &String) -> Request {
        let mut req = Request::new(
            Method::GET,
            url::Url::parse(
                format!(
                    "{}/v2/{}/blobs/{}",
                    self.registry_host_url,
                    self.reference.name(),
                    digest
                )
                .as_str(),
            )
            .unwrap(),
        );

        if let Some(v) = self.auth_token.as_ref() {
            req.headers_mut().insert(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(v.as_str()).unwrap(),
            );
        }

        req
    }
}
