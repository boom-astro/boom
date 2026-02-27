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
    ///
    /// Optional:
    /// - `SMTP_FROM_ADDRESS`: From email address (e.g., noreply@boom.example.com)
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
            tracing::info!("Email service is DISABLED (EMAIL_ENABLED=false)");
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
            tracing::info!("Email service is DISABLED (missing SMTP configuration: SMTP_SERVER)");
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
                        tracing::error!("Failed to create SMTP transport: {}", e);
                        tracing::info!(
                            "Email service is DISABLED (SMTP transport creation failed)"
                        );
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
                tracing::info!(
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

        tracing::info!("Email service is ENABLED (SMTP configured)");
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

        // Build the CTA block depending on whether a webapp URL is available
        let cta_block = if let Some(url) = webapp_url {
            format!(
                r#"<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td align="center">
                <a href="{url}/activate?email={email}&activation_code={code}"
                   style="display:inline-block;margin:18px 0 8px;padding:14px 36px;
                          background:#4f8ef7;color:#ffffff;text-decoration:none;
                          font-size:16px;font-weight:700;border-radius:6px;letter-spacing:0.5px;">
                  Activate My Account
                </a></td></tr></table>
                <p style="margin:8px 0 0;font-size:12px;color:#8899aa;text-align:center;">
                  Or paste this URL into your browser:<br>
                  <a href="{url}/activate?email={email}&activation_code={code}"
                     style="color:#4f8ef7;word-break:break-all;">
                    {url}/activate?email={email}&amp;activation_code={code}
                  </a>
                </p>"#,
                url = url,
                email = to_email,
                code = activation_code,
            )
        } else {
            format!(
                r#"<p style="margin:16px 0 8px;font-size:14px;color:#334155;">
                  Activate via <code style="font-family:monospace;">curl</code>:
                </p>
                <pre style="background:#0f172a;color:#e2e8f0;padding:18px 20px;
                            border-radius:8px;font-size:13px;line-height:1.6;
                            overflow-x:auto;white-space:pre-wrap;word-break:break-all;margin:0 0 8px;">curl -X POST https://{domain}/babamul/activate \
     -H 'Content-Type: application/json' \
     -d '{{"email":"{email}","activation_code":"{code}"}}'</pre>"#,
                domain = domain,
                email = to_email,
                code = activation_code,
            )
        };

        let email_body = format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0b1120;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#0b1120;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" border="0"
             style="max-width:560px;width:100%;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,0.5);">
        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#0b1120 0%,#1a2e50 60%,#1e3a6e 100%);padding:36px 40px 28px;text-align:center;">
            <p style="margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:3px;color:#4f8ef7;text-transform:uppercase;">Zwicky Transient Facility &middot; LSST</p>
            <h1 style="margin:0;font-size:40px;font-weight:800;color:#ffffff;letter-spacing:-0.5px;">Babamul</h1>
            <p style="margin:6px 0 0;font-size:15px;color:#7ca4d4;">A real-time multi-survey alert broker for the LSST era.</p>
          </td>
        </tr>
        <!-- Body -->
        <tr>
          <td style="padding:36px 40px 28px;">
            <p style="margin:0 0 16px;font-size:16px;color:#1e293b;">Hi there,</p>
            <p style="margin:0 0 24px;font-size:15px;line-height:1.6;color:#334155;">
              Your Babamul account request has been received.
              Use the activation code below to complete your registration.
            </p>
            <!-- Activation code pill -->
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr><td align="center">
                <div style="display:inline-block;background:#f0f6ff;border:2px solid #4f8ef7;
                            border-radius:10px;padding:18px 32px;margin:0 auto;">
                  <p style="margin:0 0 4px;font-size:16px;font-weight:700;letter-spacing:2px;color:#4f8ef7;text-transform:uppercase;padding-bottom:10px;">Activation Code</p>
                  <p style="margin:0;font-family:'Courier New',Courier,monospace;font-size:18px;font-weight:700;letter-spacing:4px;color:#0f172a;">{code}</p>
                </div>
              </td></tr>
            </table>
            <!-- CTA / curl block -->
            <div style="margin-top:10px;">{cta}</div>
            <!-- Divider -->
            <hr style="border:none;border-top:1px solid #e2e8f0;margin:32px 0;">
            <!-- What you get -->
            <p style="margin:0 0 12px;font-size:14px;font-weight:700;color:#1e293b;">After activation you will be able to:</p>
            <table cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td style="width:24px;vertical-align:top;padding-top:2px;font-size:18px;color:#4f8ef7;">&#9679;</td>
                <td style="font-size:14px;line-height:1.6;color:#334155;padding-bottom:6px;">
                  Subscribe to real-time Kafka alert streams
                </td>
              </tr>
              <tr>
                <td style="width:24px;vertical-align:top;padding-top:2px;font-size:18px;color:#4f8ef7;">&#9679;</td>
                <td style="font-size:14px;line-height:1.6;color:#334155;">Authenticate to the Babamul REST API</td>
              </tr>
            </table>
            <!-- Expiry notice -->
            <p style="margin:28px 0 0;font-size:13px;color:#64748b;background:#f8fafc;border-left:3px solid #4f8ef7;padding:10px 14px;border-radius:0 6px 6px 0;">
              &#x23F0;&nbsp; This code expires in <strong>24 hours</strong>.
              If you did not request access, you can safely ignore this email.
            </p>
          </td>
        </tr>
        <!-- Footer -->
        <tr>
          <td style="background:#f8fafc;padding:20px 40px;border-top:1px solid #e2e8f0;text-align:center;">
            <p style="margin:0;font-size:12px;color:#94a3b8;line-height:1.6;">
              BOOM &mdash; Babamul Alert Broker &bull; Caltech / ZTF / LSST<br>
              This is an automated message. Please do not reply.
            </p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"#,
            code = activation_code,
            cta = cta_block,
        );

        let email = Message::builder()
            .from(
                format!("Babamul <{}>", self.from_address)
                    .parse()
                    .map_err(|e| format!("Invalid from address: {}", e))?,
            )
            .to(to_email
                .parse()
                .map_err(|e| format!("Invalid to address: {}", e))?)
            .subject("Activate Your Babamul Account")
            .header(ContentType::TEXT_HTML)
            .body(email_body)
            .map_err(|e| format!("Failed to build email: {}", e))?;

        mailer
            .send(&email)
            .map_err(|e| format!("Failed to send email: {}", e))?;

        Ok(())
    }

    /// Send a password reset email with a raw token link
    pub fn send_password_reset_email(
        &self,
        to_email: &str,
        raw_token: &str,
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

        // Build the CTA block depending on whether a webapp URL is available
        let cta_block = if let Some(url) = webapp_url {
            format!(
                r#"<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td align="center">
                <a href="{url}/reset-password?token={token}&email={email}"
                   style="display:inline-block;margin:18px 0 8px;padding:14px 36px;
                          background:#4f8ef7;color:#ffffff;text-decoration:none;
                          font-size:16px;font-weight:700;border-radius:6px;letter-spacing:0.5px;">
                  Reset My Password
                </a></td></tr></table>
                <p style="margin:8px 0 0;font-size:12px;color:#8899aa;text-align:center;">
                  Or paste this URL into your browser:<br>
                  <a href="{url}/reset-password?token={token}&email={email}"
                     style="color:#4f8ef7;word-break:break-all;">
                    {url}/reset-password?token={token}&email={email}
                  </a>
                </p>"#,
                url = url,
                token = raw_token,
                email = to_email,
            )
        } else {
            format!(
                r#"<p style="margin:16px 0 8px;font-size:14px;color:#334155;">
                  Reset via <code style="font-family:monospace;">curl</code>:
                </p>
                <pre style="background:#0f172a;color:#e2e8f0;padding:18px 20px;
                            border-radius:8px;font-size:13px;line-height:1.6;
                            overflow-x:auto;white-space:pre-wrap;word-break:break-all;margin:0 0 8px;">curl -X POST https://{domain}/babamul/reset-password \
     -H 'Content-Type: application/json' \
     -d '{{"email":"{email}","token":"{token}","new_password":"YOUR_NEW_PASSWORD"}}'</pre>"#,
                email = to_email,
                domain = domain,
                token = raw_token,
            )
        };

        let email_body = format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0b1120;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#0b1120;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" border="0"
             style="max-width:560px;width:100%;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,0.5);">
        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#0b1120 0%,#1a2e50 60%,#1e3a6e 100%);padding:36px 40px 28px;text-align:center;">
            <p style="margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:3px;color:#4f8ef7;text-transform:uppercase;">Zwicky Transient Facility &middot; LSST</p>
            <h1 style="margin:0;font-size:40px;font-weight:800;color:#ffffff;letter-spacing:-0.5px;">Babamul</h1>
            <p style="margin:6px 0 0;font-size:15px;color:#7ca4d4;">A real-time multi-survey alert broker for the LSST era.</p>
          </td>
        </tr>
        <!-- Body -->
        <tr>
          <td style="padding:36px 40px 28px;">
            <p style="margin:0 0 16px;font-size:16px;color:#1e293b;">Hi there,</p>
            <p style="margin:0 0 24px;font-size:15px;line-height:1.6;color:#334155;">
              We received a request to reset the password for your Babamul account.
              Use the link below to choose a new password.
            </p>
            <!-- CTA / curl block -->
            <div style="margin-top:10px;">{cta}</div>
            <!-- Expiry notice -->
            <p style="margin:28px 0 0;font-size:13px;color:#64748b;background:#f8fafc;border-left:3px solid #f59e0b;padding:10px 14px;border-radius:0 6px 6px 0;">
              &#x23F0;&nbsp; This link expires in <strong>1 hour</strong>.
              If you did not request a password reset, you can safely ignore this email &mdash; your password will not change.
            </p>
          </td>
        </tr>
        <!-- Footer -->
        <tr>
          <td style="background:#f8fafc;padding:20px 40px;border-top:1px solid #e2e8f0;text-align:center;">
            <p style="margin:0;font-size:12px;color:#94a3b8;line-height:1.6;">
              BOOM &mdash; Babamul Alert Broker &bull; Caltech / ZTF / LSST<br>
              This is an automated message. Please do not reply.
            </p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"#,
            cta = cta_block,
        );

        let email = Message::builder()
            .from(
                format!("Babamul <{}>", self.from_address)
                    .parse()
                    .map_err(|e| format!("Invalid from address: {}", e))?,
            )
            .to(to_email
                .parse()
                .map_err(|e| format!("Invalid to address: {}", e))?)
            .subject("Reset Your Babamul Password")
            .header(ContentType::TEXT_HTML)
            .body(email_body)
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
