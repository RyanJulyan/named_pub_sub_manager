from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Union
from ssl import create_default_context
from ssl import SSLContext
from email import message_from_bytes
from email import parser
import smtplib
import poplib
import imaplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailProtocol(ABC):
    _server: Union[
        smtplib.SMTP_SSL, smtplib.SMTP, poplib.POP3, imaplib.IMAP4, imaplib.IMAP4_SSL
    ]

    @abstractmethod
    def send_mail(self, *args, **kwargs):
        """Send Email"""
        raise NotImplementedError("`send_mail` method not implemented")

    @abstractmethod
    def receive_mail(self, *args, **kwargs):
        """Receive Email"""
        raise NotImplementedError("`receive_mail` method not implemented")

    @abstractmethod
    def close_server(self, *args, **kwargs):
        """Receive Email"""
        raise NotImplementedError("`close_server` method not implemented")


@dataclass
class SMTPEmailProtocol(EmailProtocol):
    host: str
    user: str
    port: int = 465
    use_ssl: bool = True
    message_format: Optional[str] = None
    server: Optional[Union[smtplib.SMTP_SSL, smtplib.SMTP]] = None
    ssl_context: Optional[SSLContext] = None

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 465,
        use_ssl: bool = True,
        message_format: Optional[str] = None,
        ssl_context: Optional[SSLContext] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self.user = user
        self.host = host
        self.port = port
        self.message_format = message_format
        if use_ssl:
            if ssl_context is None:
                ssl_context = create_default_context()
            self.server = smtplib.SMTP_SSL(host, port, context=ssl_context)
        else:
            self.server = smtplib.SMTP(host, port)

        self.server.login(user, password)

    # TODO: Add attachments
    def send_mail(
        self,
        receiver_email: str,
        subject: str,
        raw_message_text: Union[str, dict],
        *args,
        **kwargs,
    ):
        """Receive Email"""
        # Prep Message
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.user
        message["To"] = receiver_email

        # Format the raw_message_text
        if self.message_format is not None:
            if isinstance(raw_message_text, dict):
                formatted_message = self.message_format.format(**raw_message_text)
            else:
                formatted_message = self.message_format.format(raw_message_text)
        else:
            formatted_message = raw_message_text

        message_text = f"{formatted_message}"

        message_html = f"""\
        <html>
        <body>
            {formatted_message}
        </body>
        </html>
        """

        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(message_text, "plain")
        part2 = MIMEText(message_html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)
        message.attach(part2)

        return self.server.sendmail(self.user, receiver_email, message.as_string())

    def receive_mail(self, *args, **kwargs):
        """Receive Email (not supported by SMTP)"""
        raise NotImplementedError(
            "it is not possible to receive emails using the SMTP protocol. If you want to send emails as well, you may want to consider using the IMAP/POP3 protocols instead."
        )

    def close_server(self, *args, **kwargs):
        """Close the SMTP server connection"""
        self.server.quit()


@dataclass
class POP3EmailProtocol(EmailProtocol):
    host: str
    user: str
    port: int = 110
    server: Optional[poplib.POP3] = None
    message_encoding: str = "utf-8"

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 110,
        message_encoding: str = "utf-8",
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self.host = host
        self.user = user
        self.port = port
        self.message_encoding = message_encoding
        self.server = poplib.POP3(host, port)
        self.server.user(user)
        self.server.pass_(password)

    def send_mail(self):
        """Send Email (not supported by POP3)"""
        raise NotImplementedError(
            "it is not possible to send emails using the POP3 protocol. If you want to send emails as well, you may want to consider using the SMTP protocol instead."
        )

    def receive_mail(self, *args, **kwargs):
        """Receive Email"""
        return_messages = []
        # Get messages from server:
        messages = [
            self.server.retr(i) for i in range(1, len(self.server.list()[1]) + 1)
        ]

        # Concat message pieces:
        messages = ["\n".join(map(bytes.decode, mssg[1])) for mssg in messages]

        # Parse message into an email object:
        messages = [parser.Parser().parsestr(mssg) for mssg in messages]
        message_dict = {}
        for message in messages:
            message_id = str(message["Message-ID"])
            message_dict_message_id = message_dict.get(message_id)
            if message_dict_message_id is None:
                if message_dict != {}:
                    return_messages.append(message_dict)
                message_dict = {}
                message_dict[message_id] = {}

            # Process all keys
            for key in message.keys():
                message_dict[message_id][key] = message[key]

            # Process body
            for part in message.walk():
                if part.get_content_type():
                    body = part.get_payload(decode=True)
                    message_dict_body = message_dict[message_id].get("body", "")
                    message_dict[message_id]["body"] = str(body) + message_dict_body

        return_messages.append(message_dict)
        return return_messages

    def close_server(self, *args, **kwargs):
        """Close the POP3 server connection"""
        self.server.quit()


@dataclass
class IMAPEmailProtocol(EmailProtocol):
    host: str
    user: str
    port: int = 143
    use_ssl: bool = False
    server: Optional[Union[imaplib.IMAP4_SSL, imaplib.IMAP4]] = None
    certfile: Optional[str] = None
    keyfile: Optional[str] = None
    ssl_context: Optional[SSLContext] = None
    select_folders: Optional[List[str]] = field(default_factory=list)

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 143,
        use_ssl: bool = False,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
        ssl_context: Optional[SSLContext] = None,
        select_folders: Optional[List[str]] = ["INBOX"],
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.use_ssl = use_ssl
        self.ssl_context = ssl_context
        self.select_folders = select_folders
        if use_ssl:
            if ssl_context is None:
                ssl_context = create_default_context()
            self.server = imaplib.IMAP4_SSL(
                host=host,
                port=port,
                keyfile=keyfile,
                certfile=certfile,
                ssl_context=ssl_context,
            )
        else:
            self.server = imaplib.IMAP4(host=host, port=port)

        # Login to the account
        self.server.login(user, password)

    def send_mail(
        self,
        *args,
        **kwargs,
    ):
        """Send Email (not supported by IMAP)"""
        raise NotImplementedError(
            "it is not possible to send emails using the IMAP protocol. If you want to send emails as well, you may want to consider using the SMTP protocol instead."
        )

    def receive_mail(self, *args, **kwargs):
        """Receive Email"""
        return_messages = []
        for select_folder in self.select_folders:
            # Select the INBOX folder
            self.server.select(select_folder)

            # Search for all messages that are not deleted
            status, messages = self.server.search(None, "NOT DELETED")

            # Get a list of all the message IDs
            message_ids = messages[0].split()

            # Fetch all the messages
            message_dict = {}
            for message_id in message_ids:
                status, msg = self.server.fetch(message_id, "(RFC822)")
                msg = message_from_bytes(msg[0][1])

                message_id = str(msg["Message-ID"])
                message_dict_message_id = message_dict.get(message_id)
                if message_dict_message_id is None:
                    if message_dict != {}:
                        return_messages.append(message_dict)
                    message_dict = {}
                    message_dict[message_id] = {}

                # Process all keys
                for key in msg.keys():
                    message_dict[message_id][key] = msg[key]

                if msg.is_multipart():
                    message_dict[message_id]["Multipart types"] = []
                    for part in msg.walk():
                        message_dict[message_id]["Multipart types"].append(
                            f"- {part.get_content_type()}"
                        )
                    multipart_payload = msg.get_payload()
                    body = ""
                    for sub_message in multipart_payload:
                        # The actual text/HTML email contents, or attachment data
                        body = body + str(sub_message.get_payload())
                    message_dict[message_id]["body"] = body
                else:  # Not a multipart message, payload is simple string
                    message_dict[message_id]["body"] = msg.get_payload()

            return_messages.append(message_dict)
        return return_messages

    def close_server(self, *args, **kwargs):
        """Close the IMAP server connection"""
        self.server.close()
        self.server.logout()


class EmailProtocols(Enum):
    SMTP = SMTPEmailProtocol
    POP3 = POP3EmailProtocol
    IMAP = IMAPEmailProtocol
