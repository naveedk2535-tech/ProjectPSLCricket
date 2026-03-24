"""
Simple SMTP email utility for alerts and notifications.
Uses Gmail app password.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import config


def send_email(to, subject, body, html=False):
    """Send an email via Gmail SMTP."""
    if not config.SMTP_EMAIL or not config.SMTP_PASSWORD:
        print("[Email] SMTP credentials not configured")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = config.SMTP_EMAIL
        msg["To"] = to if isinstance(to, str) else ", ".join(to)
        msg["Subject"] = subject

        if html:
            msg.attach(MIMEText(body, "html"))
        else:
            msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(config.SMTP_EMAIL, config.SMTP_PASSWORD)
            server.send_message(msg)

        return True

    except Exception as e:
        print(f"[Email] Error: {e}")
        return False


def send_value_bet_alert(value_bets):
    """Send alert for new value bets found."""
    if not config.ALERT_SETTINGS.get("on_value_bet"):
        return

    recipients = config.ALERT_SETTINGS.get("recipients", [])
    if not recipients:
        return

    body = "PSL Cricket Analytics — Value Bets Found\n\n"
    for vb in value_bets:
        body += f"Match: {vb['team_a']} vs {vb['team_b']}\n"
        body += f"Bet: {vb['bet_type']} | Edge: {vb['edge_pct']:.1f}%\n"
        body += f"Odds: {vb['best_odds']:.2f} | Kelly Stake: {vb['kelly_stake']:.1f}%\n\n"

    send_email(recipients, "PSL Value Bets Alert", body)


def send_critical_alert(check_name, message):
    """Send alert for critical system issues."""
    if not config.ALERT_SETTINGS.get("on_critical"):
        return

    recipients = config.ALERT_SETTINGS.get("recipients", [])
    if not recipients:
        return

    body = f"PSL Cricket Analytics — CRITICAL ALERT\n\nCheck: {check_name}\nMessage: {message}"
    send_email(recipients, f"CRITICAL: {check_name}", body)
