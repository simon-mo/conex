use reqwest::{Method, Request};

use crate::reference::DockerReference;

#[derive(Clone, Debug)]
pub struct RepoInfo {
    pub raw_tag: String, // e.g. localhost:5000/my-repo-name-image-name:latest

    pub reference: DockerReference,

    registry_host_url: String,

    pub auth_token: Option<String>,
}

impl RepoInfo {
    pub async fn from_string(source_image: String) -> RepoInfo {
        let reference = DockerReference::parse(&source_image).unwrap();

        if reference.digest().is_some() {
            unimplemented!("Digest in image reference is not supported yet.");
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

        let mut auth_token = None;

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

                // Authenticate and add token to auth_headers
                // First, read the password from the config file
                // /home/ubuntu/.docker/config.json
                // {
                //     "auths": {
                //         "https://index.docker.io/v1/": {
                //             "auth": "base64 encoded of username:password"
                //         }
                //     }
                // }
                let config = home::home_dir()
                    .unwrap()
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

                // https://username:password@auth.docker.io/token?service=registry.docker.io&scope=repository:simonmok/conex-workload:pull,push
                // TODO: make this fine-grained to only request the scope we need.
                let token_resp = reqwest::Client::new()
                    .get("https://auth.docker.io/token")
                    .basic_auth(auth[0], Some(auth[1]))
                    .query(&[
                        ("service", "registry.docker.io"),
                        (
                            "scope",
                            format!("repository:{}:pull,push", reference.name()).as_str(),
                        ),
                    ])
                    .send()
                    .await
                    .unwrap();
                assert!(token_resp.status() == 200);
                let token_resp = token_resp.json::<serde_json::Value>().await.unwrap();
                let token = token_resp["token"].as_str().unwrap();
                auth_token.replace(format!("Bearer {}", token).to_string());

                "https://registry-1.docker.io".to_string()
            }
        };

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
            http::HeaderValue::from_str("application/vnd.oci.image.manifest.v1+json").unwrap(),
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
