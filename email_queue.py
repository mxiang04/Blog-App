import threading
import queue
import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

class EmailWorker:
    def __init__(self):
        # Load SMTP settings from environment variables
        self.smtp_server = os.getenv("SMTP_SERVER", "localhost")
        self.port = int(os.getenv("SMTP_PORT", "1025"))  # Default to MailHog port
        self.username = os.getenv("SMTP_USERNAME", "")  # Optional for MailHog
        self.password = os.getenv("SMTP_PASSWORD", "")  # Optional for MailHog

        # For MailHog, we don't need to raise an error if credentials are missing
        # as it doesn't require authentication by default

        self.queue = queue.Queue()
        self.running = False
        self.thread = None
        self.logger = logging.getLogger("EmailWorker")

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._process_queue, daemon=True)
        self.thread.start()
        self.logger.info(f"Email worker started with SMTP server: {self.smtp_server}:{self.port}")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)
        self.logger.info("Email worker stopped")

    def _process_queue(self):
        while self.running:
            try:
                # Each task: (sender, recipient, subject, content)
                sender, recipient, subject, content = self.queue.get(timeout=1.0)
                self._send_email(sender, recipient, subject, content)
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing email queue: {e}")

    def _send_email(self, sender, recipient, subject, content):
        # Construct the email
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(content, "plain"))

        try:
            with smtplib.SMTP(self.smtp_server, self.port, timeout=10) as server:
                server.ehlo()
                
                # Only use these if not connecting to MailHog
                if self.smtp_server != "localhost" and self.port != 1025:
                    # Use STARTTLS if available
                    if server.has_extn('starttls'):
                        server.starttls()
                        server.ehlo()
                    
                    # Perform login only if credentials are provided and server supports AUTH
                    if self.username and self.password and server.has_extn('auth'):
                        server.login(self.username, self.password)
                
                # For MailHog, just send the message without authentication
                server.send_message(msg)
                self.logger.info(f"Email sent to {recipient}")
                
                # Add this to verify the message was sent correctly
                if self.smtp_server == "localhost" and self.port == 1025:
                    self.logger.info("Using MailHog - Check web interface at http://localhost:8025")
                    
        except Exception as e:
            self.logger.error(f"Failed to send email to {recipient}: {e}")

    def queue_email(self, sender, recipient, subject, content):
        """
        Queue an email for delivery. `sender` is the "From:" header, `recipient` is the destination.
        """
        self.queue.put((sender, recipient, subject, content))
        return True

# Global instance
email_worker = EmailWorker()