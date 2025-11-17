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
    /// Required environment variables (if email is enabled):
    /// - `SMTP_USERNAME`: SMTP username
    /// - `SMTP_PASSWORD`: SMTP password
    /// - `SMTP_SERVER`: SMTP server address (e.g., smtp.gmail.com)
    /// - `SMTP_FROM_ADDRESS`: From email address (e.g., noreply@boom.example.com)
    ///
    /// Optional:
    /// - `EMAIL_ENABLED`: Set to "false" to disable email (defaults to true if SMTP vars are set)
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
        if smtp_username.is_none() || smtp_password.is_none() || smtp_server.is_none() {
            println!(
                "Email service is DISABLED (missing SMTP configuration: SMTP_USERNAME, SMTP_PASSWORD, or SMTP_SERVER)"
            );
            return Self {
                mailer: None,
                from_address,
                enabled: false,
            };
        }

        // Build SMTP transport
        let creds = Credentials::new(smtp_username.unwrap(), smtp_password.unwrap());

        let mailer = match SmtpTransport::relay(&smtp_server.unwrap()) {
            Ok(transport) => Some(transport.credentials(creds).build()),
            Err(e) => {
                eprintln!("Failed to create SMTP transport: {}", e);
                println!("Email service is DISABLED (SMTP transport creation failed)");
                return Self {
                    mailer: None,
                    from_address,
                    enabled: false,
                };
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
    ) -> Result<(), String> {
        if !self.enabled {
            return Err("Email service is not enabled".to_string());
        }

        let mailer = self
            .mailer
            .as_ref()
            .ok_or("SMTP transport not configured")?;

        let email_body = format!(
            "Welcome to Babamul!\n\n\
             Your activation code is: {}\n\n\
             To activate your account, send a POST request to /babamul/activate with:\n\
             - email: {}\n\
             - activation_code: {}\n\n\
             After activation, you'll receive a password that you can use to:\n\
             1. Connect to Kafka streams (topics: babamul.*)\n\
             2. Authenticate to the Babamul API\n\n\
             This code will expire in 24 hours.\n\n\
             If you did not request this, please ignore this email.",
            activation_code, to_email, activation_code
        );

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
