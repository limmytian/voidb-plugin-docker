use std::fs;
use std::path::Path;
use voidb_plugin_docker::docker_capabilities;

fn main() -> anyhow::Result<()> {
    let schemas_dir = Path::new("schemas");
    fs::create_dir_all(schemas_dir)?;

    let capabilities = docker_capabilities();

    // Export capability schemas
    for cap in &capabilities {
        let input_path = schemas_dir.join(format!("{}-input.schema.json", cap.id));
        let output_path = schemas_dir.join(format!("{}-output.schema.json", cap.id));

        fs::write(&input_path, serde_json::to_string_pretty(&cap.input_schema)? + "\n")?;
        fs::write(&output_path, serde_json::to_string_pretty(&cap.output_schema)? + "\n")?;
        println!("Exported schemas for capability: {}", cap.id);
    }

    // Export profile schema matching DockerConfig
    let profile_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "DockerConnectionProfile",
        "type": "object",
        "required": ["connection"],
        "properties": {
            "connection": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["Local", "Socket", "Http", "Tls"]
                    },
                    "path": {
                        "type": "string",
                        "description": "Custom Unix socket path"
                    },
                    "url": {
                        "type": "string",
                        "description": "TCP connection URL, e.g. tcp://192.168.1.100:2376"
                    },
                    "ca_cert": {
                        "type": "string",
                        "description": "Path to CA certificate (PEM)"
                    },
                    "cert": {
                        "type": "string",
                        "description": "Path to client certificate (PEM)"
                    },
                    "key": {
                        "type": "string",
                        "description": "Path to client private key (PEM)"
                    }
                }
            },
            "timeout": {
                "type": "integer",
                "minimum": 1,
                "default": 30,
                "description": "Timeout for Docker API operations in seconds"
            }
        },
        "additionalProperties": false
    });

    let profile_path = schemas_dir.join("profile.schema.json");
    fs::write(&profile_path, serde_json::to_string_pretty(&profile_schema)? + "\n")?;
    println!("Exported profile schema to {}", profile_path.display());

    Ok(())
}
