import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(to_email, subject, body):
    """
    Gửi email qua Gmail SMTP.

    :param to_email: Email người nhận (str)
    :param subject: Tiêu đề email (str)
    :param body: Nội dung email (str)
    :return: None
    """
    # Thông tin tài khoản Gmail
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    EMAIL = "chamdaynoidaucuaerik@gmail.com"  # Thay bằng email của bạn
    PASSWORD = "wuag gbxt lhoa lele"  # Thay bằng mật khẩu ứng dụng Gmail

    try:
        # Tạo email
        msg = MIMEMultipart()
        msg["From"] = EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Kết nối đến Gmail SMTP và gửi email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Kích hoạt TLS
            server.login(EMAIL, PASSWORD)  # Đăng nhập
            server.sendmail(EMAIL, to_email, msg.as_string())  # Gửi email
            print(f"Email đã được gửi tới {to_email}")

    except Exception as e:
        print(f"Đã xảy ra lỗi khi gửi email: {e}")

def main():
  send_email('@gmail.com', "Lỗi process âccca", "error 1011")

if __name__ == "__main__":
    main()