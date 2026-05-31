import os
import smtplib
from abc import ABC, abstractmethod
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from prefect import get_run_logger
from prefect.blocks.system import Secret

class BaseNotification(ABC):
    @abstractmethod
    def send_noti(self, 
                  flow, 
                  flow_run, 
                  state
                  ) -> None:
        pass


class MailNotification(BaseNotification):
    def send_noti(self, flow, flow_run, state):
        try:
            logger = get_run_logger()
        except Exception:
            logger = None

        sender_email = os.getenv("SENDER_EMAIL")
        receiver_email = os.getenv("RECEIVER_EMAIL")

        try:
            password = Secret.load("gmail-app-password").get()
        except Exception as exc:
            message = f"Failed to load Prefect Secret block 'gmail-app-password': {exc}"
            if logger:
                logger.warning(message)
            else:
                print(message)
        
        if not all([sender_email, receiver_email, password]):
            missing = [
                name
                for name, value in {
                    "SENDER_EMAIL": sender_email,
                    "RECEIVER_EMAIL": receiver_email,
                    "EMAIL_PASSWORD or gmail-app-password block": password,
                }.items()
                if not value
            ]
            message = f"Email credentials are not fully set. Missing: {', '.join(missing)}"
            if logger:
                logger.warning(message)
            else:
                print(message)
            return
        
        prefect_url = os.getenv(
            "PREFECT_UI_URL",
            "http://prefect-server:4200"
        )
        
        msg = MIMEMultipart()
        msg['From'] = f"Prefect Alerts <{sender_email}>"
        msg['To'] = receiver_email
        msg['Subject'] = f"Prefect Alert: Flow run '{flow_run.name}' failed with state {state.name}"

        body = f"""
            Your job {flow_run.name} entered state {state.name}

            Message:
            {state.message}

            See the flow run in UI:
            {prefect_url}/flow-runs/flow-run/{flow_run.id}

            Tags: {flow_run.tags}

            Scheduled start: {flow_run.expected_start_time}
        """
        msg.attach(MIMEText(body, 'plain'))

        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, msg.as_string())
            if logger:
                logger.info("Sent failure notification email to %s", receiver_email)
        except Exception as e:
            message = f"Failed to send custom email: {e}"
            if logger:
                logger.error(message)
            else:
                print(message)
        return

