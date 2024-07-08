fn main() {
    if std::env::var("DOCS_RS").is_ok() {
        std::env::set_var("K8S_OPENAPI_ENABLED_VERSION", "1.26");
    }
}
