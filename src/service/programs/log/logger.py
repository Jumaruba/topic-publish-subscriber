import sys


class Colors:
    RESET_STYLE = '\033[0;0m'
    CYAN = '\033[0;36m'
    YELLOW = '\033[1;33m'
    GREEN = '\033[1;32m'
    RED = '\033[0;31m'
    PURPLE = '\033[0;35m'


class Logger:

    @staticmethod
    def add_color(color: str) -> None:
        sys.stdout.write(color)

    @staticmethod
    def reset_colors() -> None:
        sys.stdout.write('\033[0;0m')

    # --------------------------------------------------------------------------
    # Server logs
    # --------------------------------------------------------------------------

    @staticmethod
    def new_message(message: list) -> None:
        return
        print("\n" + "-" * 80)
        print(f"{message}")
        print("-" * 80)

    @staticmethod
    def subscription(client_id: int, topic: str) -> None:
        Logger.add_color(Colors.CYAN)
        print(f"[SUB] uid({client_id}) - t('{topic}')")
        Logger.reset_colors()

    @staticmethod
    def unsubscription(client_id: int, topic: str) -> None:
        Logger.add_color(Colors.CYAN)
        print(f"[UNSUB] uid({client_id}) - t('{topic}')")
        Logger.reset_colors()

    @staticmethod
    def publication(topic: str, message_id: int, message: str):
        print(f"[PUT] t('{topic}') - msgid({message_id}) - msg('{message}')")

    @staticmethod
    def request(client_id: int, topic: str):
        print(f"[GET] uid({client_id}) - t('{topic}')")

    @staticmethod
    def acknowledgement(client_id: int, topic: str, message_id: int):
        Logger.add_color(Colors.GREEN)
        print(f"[ACK] uid({client_id}) - t('{topic}') - msgid({message_id})")
        Logger.reset_colors()

    @staticmethod
    def info(message="", end="\n"):
        print(message, end=end)

    @staticmethod
    def success(message="", end="\n"):
        Logger.add_color(Colors.GREEN)
        print(message, end=end)
        Logger.reset_colors()

    @staticmethod
    def warning(message):
        Logger.add_color(Colors.YELLOW)
        print(message)
        Logger.reset_colors()

    @staticmethod
    def err(message):
        Logger.add_color(Colors.RED)
        print(f"[ERR] {message}")
        Logger.reset_colors()

    # --------------------------------------------------------------------------
    # Subscriber logs
    # --------------------------------------------------------------------------

    @staticmethod
    def topic_message(topic: str, msg_id: int, content: str) -> None:
        Logger.add_color(Colors.GREEN)
        print(f"[RCV] t('{topic}') - msgid({msg_id}) - msg('{content}')")
        Logger.reset_colors()

    @staticmethod
    def get(identity: int, topic: str) -> None:
        print(f"[GET] uid({identity}) - t('{topic}') ")

    @staticmethod
    def subscribe(topic: str) -> None:
        Logger.add_color(Colors.CYAN)
        print(f"[SUB] t'({topic}')")
        Logger.reset_colors()

    @staticmethod
    def unsubscribe(topic: str) -> None:
        Logger.add_color(Colors.CYAN)
        print(f"[UNSUB] t('{topic}')")
        Logger.reset_colors()

    # --------------------------------------------------------------------------
    # Publisher logs
    # --------------------------------------------------------------------------

    @staticmethod
    def put_message(pub_id: str, topic: str, msg_id: int, content: str) -> None:
        print(f"[SENT] id({pub_id}) - t('{topic}') - msgid({msg_id}) - msg('{content}')")

    @staticmethod
    def acknowledgement_pub(topic: str, message_id: int):
        Logger.add_color(Colors.GREEN)
        print(f"[ACK] t('{topic}') - msgid({message_id})")
        Logger.reset_colors()
