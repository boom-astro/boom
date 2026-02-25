//! Email service for sending transactional emails (e.g., activation codes)

use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use std::env;

#[derive(Clone)]
pub struct EmailService {
    mailer: Option<SmtpTransport>,
    from_address: String,
    enabled: bool,
}

impl EmailService {
    /// Create a new email service from environment variables
    ///
    /// Required:
    /// - `SMTP_SERVER`: SMTP server address (e.g., smtp-server.astro.caltech.edu)
    /// - `SMTP_FROM_ADDRESS`: From email address (e.g., noreply@boom.example.com)
    ///
    /// Optional:
    /// - `SMTP_USERNAME` / `SMTP_PASSWORD`: If both are set, authenticated TLS relay is used
    ///   (default port 465). If omitted, plain unauthenticated SMTP is used (default port 25).
    /// - `SMTP_PORT`: Override the port (e.g., 25, 465, 587).
    /// - `EMAIL_ENABLED`: Set to "false" to disable email entirely.
    pub fn new() -> Self {
        // Check if email should be enabled
        let enabled = env::var("EMAIL_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .unwrap_or(true);

        if !enabled {
            println!("Email service is DISABLED (EMAIL_ENABLED=false)");
            return Self {
                mailer: None,
                from_address: String::new(),
                enabled: false,
            };
        }

        // Try to load SMTP configuration
        let smtp_username = env::var("SMTP_USERNAME").ok();
        let smtp_password = env::var("SMTP_PASSWORD").ok();
        let smtp_server = env::var("SMTP_SERVER").ok();
        let from_address = env::var("SMTP_FROM_ADDRESS")
            .unwrap_or_else(|_| "noreply@boom.example.com".to_string());

        // If any SMTP config is missing, disable email
        if smtp_server.is_none() {
            println!("Email service is DISABLED (missing SMTP configuration: SMTP_SERVER)");
            return Self {
                mailer: None,
                from_address,
                enabled: false,
            };
        }

        // Optional port override (defaults to 25 for unauthenticated, 465 for authenticated)
        let smtp_port = env::var("SMTP_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok());

        // Build SMTP transport
        let mailer = match (smtp_username, smtp_password) {
            (Some(username), Some(password)) => {
                // Authenticated: use TLS relay (port 465 by default)
                let port = smtp_port.unwrap_or(465);
                match SmtpTransport::relay(&smtp_server.unwrap()) {
                    Ok(transport) => {
                        let creds = Credentials::new(username, password);
                        Some(transport.port(port).credentials(creds).build())
                    }
                    Err(e) => {
                        eprintln!("Failed to create SMTP transport: {}", e);
                        println!("Email service is DISABLED (SMTP transport creation failed)");
                        return Self {
                            mailer: None,
                            from_address,
                            enabled: false,
                        };
                    }
                }
            }
            _ => {
                // No credentials: use plain unauthenticated SMTP (port 25 by default)
                let port = smtp_port.unwrap_or(25);
                println!(
                    "SMTP_USERNAME/SMTP_PASSWORD not set — using unauthenticated plain SMTP on port {}.",
                    port
                );
                Some(
                    SmtpTransport::builder_dangerous(&smtp_server.unwrap())
                        .port(port)
                        .build(),
                )
            }
        };

        println!("Email service is ENABLED (SMTP configured)");
        Self {
            mailer,
            from_address,
            enabled: true,
        }
    }

    /// Check if email service is enabled and configured
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Send an activation email to a new Babamul user
    pub fn send_activation_email(
        &self,
        to_email: &str,
        activation_code: &str,
        domain: &str,
        webapp_url: &Option<String>,
    ) -> Result<(), String> {
        if !self.enabled {
            return Err("Email service is not enabled".to_string());
        }

        let mailer = self
            .mailer
            .as_ref()
            .ok_or("SMTP transport not configured")?;

        // if webapp url exists then use it and give instructions to activate via web link
        // else just give activation code and instructions to activate via code only
        let email_body = if let Some(url) = webapp_url {
            format!(
                "Welcome to **Babamul**!\n\n\
                 Your activation code is: **{}**\n\n\
                 To activate your account, visit the following link:\n\n\
                 {}/activate?email={}&activation_code={}\n\n\
                 After activation, you'll receive a password that you can use to:\n\
                 1. Connect to Kafka streams (topics: babamul.*)\n\
                 2. Authenticate to the Babamul API\n\n\
                 This code will expire in 24 hours.\n\n\
                 If you did not request this, please ignore this email.",
                activation_code, url, to_email, activation_code
            )
        } else {
            format!(
                "Welcome to **Babamul**!\n\n\
                 Your activation code is: **{}**\n\n\
                 To activate your account, use the following `curl` command:\n\n\
                 ```bash\n\
                 curl -X POST https://{}/babamul/activate \\\n\
                 -H 'Content-Type: application/json' \\\n\
                 -d '{{\"email\":\"{}\",\"activation_code\":\"{}\"}}'\n\
                 ```\n\n\
                 After activation, you'll receive a password that you can use to:\n\
                 1. Connect to Kafka streams (topics: babamul.*)\n\
                 2. Authenticate to the Babamul API\n\n\
                 This code will expire in 24 hours.\n\n\
                 If you did not request this, please ignore this email.",
                activation_code, domain, to_email, activation_code
            )
        };

        let email = Message::builder()
            .from(
                format!("BOOM Babamul <{}>", self.from_address)
                    .parse()
                    .map_err(|e| format!("Invalid from address: {}", e))?,
            )
            .to(to_email
                .parse()
                .map_err(|e| format!("Invalid to address: {}", e))?)
            .subject("Activate Your Babamul Account")
            .header(ContentType::TEXT_PLAIN)
            .body(email_body)
            .map_err(|e| format!("Failed to build email: {}", e))?;

        mailer
            .send(&email)
            .map_err(|e| format!("Failed to send email: {}", e))?;

        Ok(())
    }

    /// Send a test email to verify SMTP configuration
    pub fn send_test_email(&self, to_email: &str) -> Result<(), String> {
        if !self.enabled {
            return Err("Email service is not enabled".to_string());
        }

        let mailer = self
            .mailer
            .as_ref()
            .ok_or("SMTP transport not configured")?;

        let email = Message::builder()
            .from(
                format!("BOOM Test <{}>", self.from_address)
                    .parse()
                    .map_err(|e| format!("Invalid from address: {}", e))?,
            )
            .to(to_email
                .parse()
                .map_err(|e| format!("Invalid to address: {}", e))?)
            .subject("BOOM Email Service Test")
            .header(ContentType::TEXT_PLAIN)
            .body("This is a test email from the BOOM email service. If you received this, SMTP is configured correctly.".to_string())
            .map_err(|e| format!("Failed to build email: {}", e))?;

        mailer
            .send(&email)
            .map_err(|e| format!("Failed to send email: {}", e))?;

        Ok(())
    }
}

impl Default for EmailService {
    fn default() -> Self {
        Self::new()
    }
}
