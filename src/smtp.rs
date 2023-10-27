//
// Copyright (c) 2023 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::crypto::keyring_get_secret;
use anyhow::{bail, Result};
use lettre::{
    message::{header, MultiPart, SinglePart},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tera::{Context, Tera};

// Simple email template customized for kip was created by silverbacksays:
// https://thwack.solarwinds.com/product-forums/the-orion-platform/f/alert-lab/2946/sample-html-css-alert-template
const EMAIL: &str = r#"<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    <title>kip alert: {{ alert_title }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <style> body {background-color: #f0f0f0;font-family: Arial, sans-serif;color: #404040;}.center {text-align: center;}.tight {padding: 15px 30px;}td {padding: 20px 50px 30px 50px;}td.notification {padding: 10px 50px 30px 50px;}small, .small {font-size: 12px;}.footer {padding: 15px 30px;}.footer p {font-size: 12px;margin: 0;color: #606060;}a, a:hover, a:visited {color: #000000;text-decoration: underline;}h1, h2 {font-size: 22px;color: #404040;font-weight: normal;}p {font-size: 15px;color: #606060;}.general {background-color: white;}.notification h1 {font-size: 26px;color: #000000;font-weight: normal;}.notification p {font-size: 18px;}.notification p.small {font-size: 14px;}.icon {width: 32px;height: 32px;line-height: 32px;display: inline-block;text-align: center;border-radius: 16px;margin-right: 10px;}.failure {border-top: 20px #b02020 solid;background-color: #db9c9b;}.critical {border-top: 20px #c05050 solid;background-color: #e2afae;}.warning {border-top: 20px #c08040 solid;background-color: #e0c4aa;}.healthy {border-top: 20px #80c080 solid;background-color: #c6e2c3;}.information {border-top: 20px #50a0c0 solid;background-color: #b5d5e2;}.failure p {color: #3d120f;}.critical p {color: #3d211f;}.warning p {color: #44311c;}.healthy p {color: #364731;}.information p {color: #273c47;}.failure .icon {background-color: #b02020;color: #ffffff;}.critical .icon {background-color: #c05050;color: #ffffff;font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;}.warning .icon {background-color: #c08040;color: #ffffff;font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;}.healthy .icon {background-color: #80c080;color: #ffffff;}.information .icon {background-color: #50a0c0;color: #ffffff;font-family: Georgia, "Times New Roman", Times, serif;font-style: italic;}.content {width: 600px;}@media only screen and (max-width: 600px) {.content {width: 100%;}}@media only screen and (max-width: 400px) {td {padding: 15px 25px;}h1, h2 {font-size: 20px;}p {font-size: 13px;}small, .small {font-size: 11px;}td.notification {text-align: center;padding: 10px 25px 15px 25px;}.notification h1 {font-size: 22px;}.notification p {font-size: 16px;}.notification p.small {font-size: 12px;}.icon {display: block;margin: 0 auto 10px auto;}}</style>
</head>

<body style="margin: 0; padding: 0">
    <table style="border: none" cellpadding="0" cellspacing="0" width="100%">
        <tr>
            <td style="padding: 15px 0">
                <table style="border: none; margin-left: auto; margin-right: auto" cellpadding="0" cellspacing="0" width="600" class="content">
                    <!-- Start: Small header text in pale grey email background -->
                    <tr>
                        <td class="center tight large">kip backup alerts</td>
                    </tr>
                    <!-- End: Small header text in pale grey email background -->

                    {% if success %}
                        <!-- Start: Healthy Notification -->
                        <tr>
                            <td class="healthy notification">
                                <h1><span class="icon">&check;</span>Success</h1>
                                <p>{{ alert_title }}</p>
                            </td>
                        </tr>
                        <!-- End: Healthy Notification -->
                    {% elif information %}
                        <!-- Start: Informational Notification -->
                        <tr>
                            <td class="information notification">
                                <h1><span class="icon">i</span>Information</h1>
                                <p>{{ alert_title }}</p>
                            </td>
                        </tr>
                        <!-- End: Informational Notification -->
                    {% elif warning %}
                        <!-- Start: Warning Notification -->
                        <tr>
                            <td class="warning notification">
                                <h1><span class="icon">&quest;</span>Warning</h1>
                                <p>{{ alert_title }}</p>
                            </td>
                        </tr>
                        <!-- End: Warning Notification -->
                    {% elif error %}
                        <!-- Start: Failure Notification -->
                        <tr>
                            <td class="failure notification">
                                <h1><span class="icon">&times;</span>Error</h1>
                                <p>{{ alert_title }}</p>
                            </td>
                        </tr>
                        <!-- End: Failure Notification -->
                    {% else %}
                        <p>ERROR: rendering email</p>
                    {% endif %}

                    <!-- Start: White block with text content -->
                    <tr>
                        <td class="general center">
                            {% for log in alert_logs %}
                                <p class="small">{{ log }}</p>
                            {% endfor %}
                            <p class="small">Please enabling debugging and review kip client logs for further detailed information.</p>
                        </td>
                    </tr>
                    <!-- End: White block with text content -->

                    <!-- Start: Footer block in pale grey email background -->
                    <tr>
                        <td class="footer">
                            <p> © 2023 | Powered by <a href="https://github.com/ciehanski/kip">kip v1.0.5</a> created by <a href="https://www.ciehanski.com">Ryan Ciehanski</a></p>
                        </td>
                    </tr>
                    <!-- End: Footer block in pale grey email background -->
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"#;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipSmtpOpts {
    pub username: String,
    pub smtp_host: String,
    pub protocol: KipSmtpProtocols,
    pub recipient: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KipSmtpProtocols {
    TLS,
    StartTLS,
    Smtp,
    Localhost,
}

#[derive(Debug)]
pub struct KipEmail {
    pub title: String,
    pub alert_type: KipAlertType,
    pub alert_logs: Vec<String>,
}

#[derive(Debug)]
pub enum KipAlertType {
    Success,
    Information,
    Warning,
    Error,
}

pub async fn send_email(opts: KipSmtpOpts, email: KipEmail) -> Result<()> {
    // Get SMTP password from keyring
    let smtp_pass = keyring_get_secret("com.ciehanski.kip.smtp")?;
    // Create SMTP credentials from stored config username and smtp_pass
    let smtp_creds = Credentials::new(opts.username, smtp_pass);
    // Build email
    let msg = build_email(&opts.recipient, email)?;

    // Connect to server & send
    match opts.protocol {
        KipSmtpProtocols::TLS => {
            let mailer: AsyncSmtpTransport<Tokio1Executor> =
                AsyncSmtpTransport::<Tokio1Executor>::relay(&opts.smtp_host)?
                    .timeout(Some(Duration::from_secs(10)))
                    .credentials(smtp_creds)
                    .build();

            // Test connection to server
            match mailer.test_connection().await {
                Ok(true) => {
                    mailer.send(msg).await?;
                }
                Ok(false) => {
                    bail!(
                        "unable to connect to {} via a TLS wrapped connection",
                        &opts.smtp_host
                    );
                }
                Err(err) => {
                    bail!(
                        "unable to connect to {} via a TLS wrapped connection: {err}",
                        &opts.smtp_host
                    );
                }
            }
        }
        KipSmtpProtocols::StartTLS => {
            let mailer: AsyncSmtpTransport<Tokio1Executor> =
                AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&opts.smtp_host)?
                    .timeout(Some(Duration::from_secs(10)))
                    .credentials(smtp_creds)
                    .build();

            // Test connection to server
            match mailer.test_connection().await {
                Ok(true) => {
                    // Send
                    mailer.send(msg).await?;
                }
                Ok(false) => {
                    bail!(
                        "Couldn't connect to {} via a StartTLS wrapped connection",
                        &opts.smtp_host
                    );
                }
                Err(err) => {
                    bail!(
                        "Couldn't connect to {} via a StartTLS wrapped connection: {err}",
                        &opts.smtp_host
                    );
                }
            }
        }
        KipSmtpProtocols::Smtp => {
            let mailer: AsyncSmtpTransport<Tokio1Executor> =
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&opts.smtp_host)
                    .timeout(Some(Duration::from_secs(10)))
                    .credentials(smtp_creds)
                    .build();

            // Test connection to server
            match mailer.test_connection().await {
                Ok(true) => {
                    // Send
                    mailer.send(msg).await?;
                }
                Ok(false) => {
                    bail!(
                        "Couldn't connect to {} via an SMTP connection",
                        &opts.smtp_host
                    );
                }
                Err(err) => {
                    bail!(
                        "Couldn't connect to {} via an SMTP connection: {err}",
                        &opts.smtp_host
                    );
                }
            }
        }
        KipSmtpProtocols::Localhost => {
            let mailer: AsyncSmtpTransport<Tokio1Executor> =
                AsyncSmtpTransport::<Tokio1Executor>::unencrypted_localhost();

            // Test connection to localhost
            match mailer.test_connection().await {
                Ok(true) => {
                    // Send
                    mailer.send(msg).await?;
                }
                Ok(false) => {
                    bail!("Couldn't connect to 127.0.0.1:25");
                }
                Err(err) => {
                    bail!("Couldn't connect to 127.0.0.1:25: {err}");
                }
            }
        }
    }

    Ok(())
}

fn build_email(recipient: &str, email: KipEmail) -> Result<Message> {
    let mut templates = Tera::default();
    templates.add_raw_template("email.html", EMAIL)?;
    templates.autoescape_on(vec![".html"]);

    // Inject varibales into HTML using Tera context
    let mut tera_ctx = Context::new();
    tera_ctx.insert("alert_title", &email.title);
    tera_ctx.insert("alert_logs", &email.alert_logs);

    // The html we want to send.
    match email.alert_type {
        KipAlertType::Success => {
            tera_ctx.insert("success", &true);
            tera_ctx.insert("information", &false);
            tera_ctx.insert("warning", &false);
            tera_ctx.insert("error", &false);
        }
        KipAlertType::Information => {
            tera_ctx.insert("success", &false);
            tera_ctx.insert("information", &true);
            tera_ctx.insert("warning", &false);
            tera_ctx.insert("error", &false);
        }
        KipAlertType::Warning => {
            tera_ctx.insert("success", &false);
            tera_ctx.insert("information", &false);
            tera_ctx.insert("warning", &true);
            tera_ctx.insert("error", &false);
        }
        KipAlertType::Error => {
            tera_ctx.insert("success", &false);
            tera_ctx.insert("information", &false);
            tera_ctx.insert("warning", &false);
            tera_ctx.insert("error", &true);
        }
    }

    // Construct the full email
    let msg = Message::builder()
        .from("kip backups <kip@ciehanski.com>".parse()?)
        .to(recipient.parse()?)
        .subject(format!("kip alert: {}", email.title))
        .multipart(
            MultiPart::alternative() // This is composed of two parts.
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_PLAIN)
                        .body(email.title.to_string()), // Every message should have a plain text fallback.
                )
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_HTML)
                        .body(templates.render("email.html", &tera_ctx)?),
                ),
        )?;
    Ok(msg)
}
